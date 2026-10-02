package dev.jobqueue.worker;

import dev.jobqueue.config.JobQueueProperties;
import dev.jobqueue.core.Job;
import dev.jobqueue.handler.HandlerRegistry;
import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import dev.jobqueue.handler.NonRetryableException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/** Runs one claimed job on the calling thread and records the outcome. Never throws. */
@Component
public class JobExecutor {

  private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);

  /** Placeholder until phase 4 introduces exponential backoff with jitter. */
  static final Duration PLACEHOLDER_RETRY_DELAY = Duration.ofSeconds(1);

  private static final int MAX_ERROR_CHARS = 1_000;
  private static final int MAX_TRACE_CHARS = 4_000;

  private final HandlerRegistry registry;
  private final JobClaimRepository repo;
  private final String workerId;

  public JobExecutor(
      HandlerRegistry registry, JobClaimRepository repo, JobQueueProperties properties) {
    this.registry = registry;
    this.repo = repo;
    this.workerId = properties.workerId();
  }

  public void run(Job job) {
    MDC.put("job_id", job.id().toString());
    MDC.put("worker_id", workerId);
    try {
      execute(job);
    } catch (RuntimeException e) {
      // Recording the outcome failed (e.g. DB down). The job stays RUNNING until its lease
      // expires and the reaper (phase 5) returns it to the queue: at-least-once, nothing lost.
      log.error("could not record outcome of job {} attempt {}", job.id(), job.attempts(), e);
    } finally {
      MDC.remove("job_id");
      MDC.remove("worker_id");
    }
  }

  private void execute(Job job) {
    Optional<JobHandler> handler = registry.find(job.type());
    if (handler.isEmpty()) {
      recordFailure(
          job, false, "FAILED_NON_RETRYABLE", "no handler registered for " + job.type(), null);
      return;
    }
    log.info("starting {} attempt {}/{}", job.type(), job.attempts(), job.maxAttempts());
    try {
      handler
          .get()
          .handle(
              new JobContext(
                  job.id(),
                  job.type(),
                  job.attempts(),
                  job.maxAttempts(),
                  job.payload(),
                  workerId));
    } catch (NonRetryableException e) {
      recordFailure(job, false, "FAILED_NON_RETRYABLE", e.getMessage(), e);
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      recordFailure(job, true, "FAILED_RETRYABLE", "interrupted", e);
      return;
    } catch (Exception e) {
      // RetryableException and anything unknown: retry by default.
      recordFailure(job, true, "FAILED_RETRYABLE", String.valueOf(e.getMessage()), e);
      return;
    }
    if (repo.complete(job.id(), workerId, job.attempts())) {
      log.info("succeeded {}", job.type());
    } else {
      log.warn("lost ownership of job {} before completing; result discarded", job.id());
    }
  }

  private void recordFailure(
      Job job, boolean retryable, String outcome, String message, Throwable error) {
    var status =
        repo.fail(
            job.id(),
            workerId,
            job.attempts(),
            retryable,
            PLACEHOLDER_RETRY_DELAY,
            outcome,
            truncate(message, MAX_ERROR_CHARS),
            error == null ? null : truncate(stackTrace(error), MAX_TRACE_CHARS));
    if (status.isPresent()) {
      log.warn("failed {} attempt {}: {} -> {}", job.type(), job.attempts(), message, status.get());
    } else {
      log.warn("lost ownership of job {} before recording failure; ignored", job.id());
    }
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
