package dev.jobqueue.worker;

import dev.jobqueue.config.ConditionalOnRole;
import dev.jobqueue.config.JobQueueProperties;
import dev.jobqueue.config.Role;
import dev.jobqueue.core.Job;
import dev.jobqueue.metrics.JobMetrics;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Polls for jobs, claims them in batches and runs them concurrently.
 *
 * <p><b>Concurrency model: a virtual thread per job, bounded by a semaphore.</b> Handlers are
 * dominated by blocking I/O (SMTP, HTTP, slow queries), which virtual threads handle cheaply, and
 * the semaphore (not a pool size) is what caps in-flight work so a slow downstream cannot make us
 * claim more than we can run. Trade-off: a handler that is pure CPU work would be better served by
 * a bounded platform-thread pool sized to the core count. Here {@code concurrency} is the cap
 * either way.
 *
 * <p>The poller only claims as many jobs as there are free slots, so a claimed job never waits in a
 * local queue while its lease runs down.
 */
@Component
@ConditionalOnRole(Role.WORKER)
public class WorkerLoop implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(WorkerLoop.class);

  private final JobClaimRepository repo;
  private final JobExecutor executor;
  private final LeaseRepository leases;
  private final InFlightJobs inFlight;
  private final JobMetrics metrics;
  private final Heartbeater heartbeater;
  private final WorkerProperties props;
  private final String workerId;
  private final Semaphore slots;
  private final ExecutorService jobThreads =
      Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("job-", 0).factory());

  private final java.util.concurrent.CountDownLatch stopSignal =
      new java.util.concurrent.CountDownLatch(1);

  /** Released whenever a job finishes, so a saturated poller refills the slot immediately. */
  private final Semaphore slotFreed = new Semaphore(0);

  private volatile boolean running;
  private Thread poller;

  public WorkerLoop(
      JobClaimRepository repo,
      JobExecutor executor,
      LeaseRepository leases,
      InFlightJobs inFlight,
      JobMetrics metrics,
      WorkerProperties props,
      JobQueueProperties queueProperties) {
    this.repo = repo;
    this.executor = executor;
    this.leases = leases;
    this.inFlight = inFlight;
    this.metrics = metrics;
    this.props = props;
    this.workerId = queueProperties.workerId();
    this.heartbeater =
        new Heartbeater(
            leases, inFlight, metrics, workerId, props.leaseDuration(), props.heartbeatInterval());
    this.slots = new Semaphore(props.concurrency());
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    running = true;
    heartbeater.start();
    poller = new Thread(this::pollLoop, "job-poller");
    poller.start();
    log.info(
        "worker {} started: concurrency={} batchSize={} queues={}",
        workerId,
        props.concurrency(),
        props.batchSize(),
        props.queues().isEmpty() ? "all" : props.queues());
  }

  private void pollLoop() {
    MDC.put("worker_id", workerId);
    Duration idleSleep = props.pollInterval();
    while (running) {
      int claimed;
      try {
        claimed = pollOnce();
      } catch (RuntimeException e) {
        log.error("unexpected error in poll loop; continuing", e);
        claimed = 0;
      }
      boolean drained = claimed == props.batchSize();
      if (claimed > 0) {
        idleSleep = props.pollInterval();
      } else if (claimed == 0) {
        idleSleep = min(idleSleep.multipliedBy(2), props.maxPollInterval()); // back off while idle
      }
      if (drained) {
        continue; // a full batch suggests more is waiting: poll again immediately
      }
      try {
        // claimed == -1 means all slots busy: re-check soon rather than backing off.
        if (awaitNextPoll(claimed < 0 ? props.pollInterval() : idleSleep)) {
          return;
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  /**
   * Waits up to {@code max} before the next poll, but wakes early when a job finishes (a slot
   * freed) or when stop() is called. Without the early wake-up a saturated worker would refill its
   * slots only once per poll interval, capping throughput at {@code concurrency / pollInterval}
   * jobs per second however fast the handlers are: a bug the load test found.
   *
   * <p>Waiting on a signal instead of {@code Thread.sleep} also lets stop() end the wait without
   * interrupting the poller, which could otherwise land in the middle of a JDBC call.
   *
   * @return true if the worker is stopping
   */
  private boolean awaitNextPoll(Duration max) throws InterruptedException {
    if (stopSignal.getCount() > 0) {
      slotFreed.tryAcquire(max.toNanos(), TimeUnit.NANOSECONDS);
      slotFreed.drainPermits();
    }
    return stopSignal.getCount() == 0;
  }

  /**
   * @return jobs claimed, 0 if none (or a DB error), -1 if no free slot
   */
  int pollOnce() {
    int want = Math.min(props.batchSize(), slots.availablePermits());
    if (want == 0 || !slots.tryAcquire(want)) {
      return -1;
    }
    List<Job> jobs;
    try {
      jobs = repo.claim(workerId, want, props.leaseDuration(), props.queues());
    } catch (RuntimeException e) {
      slots.release(want);
      log.warn("claim failed, will retry: {}", e.toString());
      return 0;
    }
    slots.release(want - jobs.size());
    jobs.forEach(
        j ->
            metrics.enqueueToStart(j.type(), java.time.Duration.between(j.runAt(), j.updatedAt())));
    jobs.stream()
        .sorted(Comparator.comparingInt(Job::priority).reversed().thenComparing(Job::runAt))
        .forEach(this::dispatch);
    return jobs.size();
  }

  private void dispatch(Job job) {
    jobThreads.execute(
        () -> {
          try {
            executor.run(job);
          } finally {
            slots.release();
            slotFreed.release();
          }
        });
  }

  /**
   * Graceful shutdown:
   *
   * <ol>
   *   <li>stop claiming new jobs;
   *   <li>keep heartbeating while in-flight jobs finish, up to {@code shutdown-grace-period};
   *   <li>at the deadline, mark what is still running as released, interrupt it, and hand its
   *       leases back to the queue right away (no attempt consumed), so another worker picks the
   *       jobs up in seconds instead of waiting for the lease to expire.
   * </ol>
   *
   * Nothing is lost either way: a job either finishes, or returns to PENDING.
   */
  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    running = false; // 1. stop claiming
    stopSignal.countDown();
    slotFreed.release(); // wake the poller if it is waiting
    try {
      // Join the poller BEFORE closing the job executor: a poll already in progress may have just
      // claimed jobs, and they must still be dispatched (and then drained below), not orphaned.
      poller.join(10_000);
      jobThreads.shutdown();
      int inFlightAtStop = inFlight.count();
      log.info("worker {} stopping: waiting for {} in-flight job(s)", workerId, inFlightAtStop);
      boolean drained =
          jobThreads.awaitTermination(
              props.shutdownGracePeriod().toMillis(), TimeUnit.MILLISECONDS);
      if (!drained) {
        releaseStragglers();
        jobThreads.shutdownNow();
        jobThreads.awaitTermination(5, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      heartbeater.stop();
    }
    log.info("worker {} stopped", workerId);
  }

  private void releaseStragglers() {
    List<InFlightJobs.Handle> remaining = inFlight.snapshot();
    log.warn(
        "shutdown grace period elapsed with {} job(s) still running: releasing their leases",
        remaining.size());
    remaining.forEach(InFlightJobs.Handle::markReleased); // before interrupting: see JobExecutor
    int released;
    try {
      released =
          leases.release(workerId, remaining.stream().map(InFlightJobs.Handle::lease).toList());
    } catch (RuntimeException e) {
      // Not fatal: unreleased leases simply expire and the reaper requeues those jobs.
      log.error("could not release leases; the reaper will reclaim them after expiry", e);
      released = 0;
    }
    remaining.forEach(h -> h.thread().interrupt());
    log.info("released {} lease(s) back to the queue", released);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private static Duration min(Duration a, Duration b) {
    return a.compareTo(b) <= 0 ? a : b;
  }
}
