package dev.jobqueue.worker;

import dev.jobqueue.config.ConditionalOnRole;
import dev.jobqueue.config.JobQueueProperties;
import dev.jobqueue.config.Role;
import dev.jobqueue.core.Job;
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
  private final WorkerProperties props;
  private final String workerId;
  private final Semaphore slots;
  private final ExecutorService jobThreads =
      Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("job-", 0).factory());

  private volatile boolean running;
  private Thread poller;

  public WorkerLoop(
      JobClaimRepository repo,
      JobExecutor executor,
      WorkerProperties props,
      JobQueueProperties queueProperties) {
    this.repo = repo;
    this.executor = executor;
    this.props = props;
    this.workerId = queueProperties.workerId();
    this.slots = new Semaphore(props.concurrency());
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    running = true;
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
      int claimed = pollOnce();
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
        Thread.sleep((claimed < 0 ? props.pollInterval() : idleSleep).toMillis());
      } catch (InterruptedException e) {
        return;
      }
    }
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
          }
        });
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    running = false; // stop claiming
    poller.interrupt();
    jobThreads.shutdown();
    try {
      poller.join(5_000);
      if (!jobThreads.awaitTermination(
          props.shutdownGracePeriod().toMillis(), TimeUnit.MILLISECONDS)) {
        // Phase 5: interrupt stragglers and release their leases. For now they stay RUNNING.
        log.warn("shutdown grace period elapsed with jobs still running");
        jobThreads.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.info("worker {} stopped", workerId);
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private static Duration min(Duration a, Duration b) {
    return a.compareTo(b) <= 0 ? a : b;
  }
}
