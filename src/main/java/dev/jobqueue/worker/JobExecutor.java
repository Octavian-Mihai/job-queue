package dev.jobqueue.worker;

import dev.jobqueue.config.JobQueueProperties;
import dev.jobqueue.core.Job;
import dev.jobqueue.handler.HandlerRegistry;
import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import dev.jobqueue.metrics.JobMetrics;
import dev.jobqueue.retry.RetryPolicies;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/** Runs one claimed job on the calling thread and records the outcome. Never throws. */
@Component
public class JobExecutor {

  private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);

  private static final int MAX_ERROR_CHARS = 1_000;
  private static final int MAX_TRACE_CHARS = 4_000;

  /** One daemon thread that fires execution timeouts by interrupting the job's thread. */
  private static final ScheduledExecutorService TIMEOUTS = newTimeoutScheduler();

  private final HandlerRegistry registry;
  private final JobClaimRepository repo;
  private final RetryPolicies policies;
  private final InFlightJobs inFlight;
  private final JobMetrics metrics;
  private final String workerId;

  public JobExecutor(
      HandlerRegistry registry,
      JobClaimRepository repo,
      RetryPolicies policies,
      InFlightJobs inFlight,
      JobMetrics metrics,
      JobQueueProperties properties) {
    this.registry = registry;
    this.repo = repo;
    this.policies = policies;
    this.inFlight = inFlight;
    this.metrics = metrics;
    this.workerId = properties.workerId();
  }

  public void run(Job job) {
    MDC.put("job_id", job.id().toString());
    MDC.put("worker_id", workerId);
    InFlightJobs.Handle handle = inFlight.register(job, Thread.currentThread());
    try {
      execute(job, handle);
    } catch (RuntimeException e) {
      // Recording the outcome failed (e.g. DB down). The job stays RUNNING until its lease
      // expires and the reaper (phase 5) returns it to the queue: at-least-once, nothing lost.
      log.error("could not record outcome of job {} attempt {}", job.id(), job.attempts(), e);
    } finally {
      inFlight.unregister(handle);
      MDC.remove("job_id");
      MDC.remove("worker_id");
    }
  }

  /** How the handler call ended: {@code error == null} means it returned normally. */
  private record Result(Throwable error, boolean timedOut) {}

  private void execute(Job job, InFlightJobs.Handle handle) {
    Optional<JobHandler> handler = registry.find(job.type());
    if (handler.isEmpty()) {
      recordFailure(
          job,
          FailureClassifier.NON_RETRYABLE,
          "no handler registered for " + job.type(),
          null,
          Duration.ZERO);
      return;
    }
    RetryPolicies.Resolved policy = policies.resolve(job.type());
    log.info("starting {} attempt {}/{}", job.type(), job.attempts(), job.maxAttempts());

    long startNanos = System.nanoTime();
    Result result = invoke(handler.get(), job, policy.executionTimeout(), handle);
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

    if (result.error() == null) {
      if (repo.complete(job.id(), workerId, job.attempts())) {
        metrics.attempt(job.type(), "SUCCEEDED");
        metrics.executionTime(job.type(), "succeeded", elapsed);
        log.info("succeeded {}", job.type());
      } else {
        metrics.fenced("complete");
        log.warn("lost ownership of job {} before completing; result discarded", job.id());
      }
    } else if (handle.released()) {
      // Shutdown interrupted this handler and is handing the lease back (attempt not consumed);
      // recording a failure here would wrongly burn an attempt.
      log.info("interrupted by shutdown; lease released without recording a failure");
    } else if (result.timedOut()) {
      recordFailure(
          job,
          new FailureClassifier.Classification(true, "TIMED_OUT"),
          "execution timed out after " + policy.executionTimeout().toMillis() + " ms",
          result.error(),
          elapsed);
    } else {
      recordFailure(
          job,
          FailureClassifier.classify(result.error()),
          String.valueOf(result.error().getMessage()),
          result.error(),
          elapsed);
    }
  }

  /**
   * Calls the handler under an execution timeout. On timeout the job's thread is interrupted; a
   * handler that honours interruption fails fast. One that ignores it keeps its slot busy (the
   * thread cannot be killed), which bounds concurrency and prevents a second, overlapping run of
   * the same job. If the handler returns normally despite a late interrupt, the work is done and
   * counts as success.
   */
  private Result invoke(JobHandler handler, Job job, Duration timeout, InFlightJobs.Handle handle) {
    AtomicBoolean timedOut = handle.timedOut();
    Thread self = Thread.currentThread();
    ScheduledFuture<?> guard =
        TIMEOUTS.schedule(
            () -> {
              timedOut.set(true);
              self.interrupt();
            },
            timeout.toMillis(),
            TimeUnit.MILLISECONDS);
    try {
      handler.handle(
          new JobContext(
              job.id(), job.type(), job.attempts(), job.maxAttempts(), job.payload(), workerId));
      return new Result(null, false);
    } catch (Exception e) { // includes InterruptedException (timeout, or shutdown in phase 5)
      return new Result(e, timedOut.get());
    } finally {
      if (!guard.cancel(false)) {
        awaitQuietly(guard); // the timeout task already started: let it finish its interrupt
      }
      Thread.interrupted(); // never carry a stale interrupt into the JDBC calls that follow
    }
  }

  private void recordFailure(
      Job job,
      FailureClassifier.Classification kind,
      String message,
      Throwable error,
      Duration elapsed) {
    Duration delay =
        policies.resolve(job.type()).backoff().delay(job.attempts(), ThreadLocalRandom.current());
    var status =
        repo.fail(
            job.id(),
            workerId,
            job.attempts(),
            kind.retryable(),
            delay,
            kind.outcome(),
            truncate(message, MAX_ERROR_CHARS),
            error == null ? null : truncate(stackTrace(error), MAX_TRACE_CHARS));
    if (status.isPresent()) {
      metrics.attempt(job.type(), kind.outcome());
      metrics.executionTime(
          job.type(), kind.outcome().equals("TIMED_OUT") ? "timed_out" : "failed", elapsed);
      if (status.get() == dev.jobqueue.core.JobStatus.PENDING) {
        metrics.retried(job.type());
      } else {
        metrics.dead(
            job.type(),
            kind.retryable() ? DeadReason.MAX_ATTEMPTS_EXCEEDED : DeadReason.NON_RETRYABLE);
      }
      log.warn(
          "failed {} attempt {}/{} ({}): {} -> {}",
          job.type(),
          job.attempts(),
          job.maxAttempts(),
          kind.outcome(),
          message,
          status.get());
    } else {
      metrics.fenced("fail");
      log.warn("lost ownership of job {} before recording failure; ignored", job.id());
    }
  }

  private static void awaitQuietly(ScheduledFuture<?> future) {
    try {
      future.get(1, TimeUnit.SECONDS);
    } catch (Exception ignored) {
      // cancelled or already done: nothing to wait for
    }
  }

  private static ScheduledExecutorService newTimeoutScheduler() {
    var pool =
        new ScheduledThreadPoolExecutor(
            1,
            r -> {
              Thread t = new Thread(r, "job-timeout");
              t.setDaemon(true);
              return t;
            });
    pool.setRemoveOnCancelPolicy(true);
    return pool;
  }

  private static String stackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  private static String truncate(String s, int max) {
    return s == null || s.length() <= max ? s : s.substring(0, max);
  }
}
