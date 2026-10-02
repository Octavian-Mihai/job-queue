package dev.jobqueue.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.stereotype.Component;

/**
 * All event metrics in one place. Tag values are bounded (job type is limited to registered
 * handlers; outcomes and reasons are fixed enums), so cardinality stays small.
 *
 * <p>Prometheus names: {@code jobqueue_attempts_total}, {@code jobqueue_jobs_retried_total}, {@code
 * jobqueue_jobs_dead_total}, {@code jobqueue_jobs_enqueued_total}, {@code
 * jobqueue_leases_reclaimed_total}, {@code jobqueue_fenced_total}, {@code
 * jobqueue_heartbeat_lost_total}, histograms {@code jobqueue_job_execution_seconds} and {@code
 * jobqueue_job_enqueue_to_start_seconds}.
 */
@Component
public class JobMetrics {

  private static final Duration MIN = Duration.ofMillis(1);
  private static final Duration MAX = Duration.ofMinutes(10);

  private final MeterRegistry registry;

  public JobMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  /** A finished attempt: SUCCEEDED, FAILED_RETRYABLE, FAILED_NON_RETRYABLE or TIMED_OUT. */
  public void attempt(String type, String outcome) {
    Counter.builder("jobqueue.attempts")
        .description("Finished execution attempts by outcome")
        .tag("type", type)
        .tag("outcome", outcome)
        .register(registry)
        .increment();
  }

  /** A failed attempt that sent the job back to PENDING for another try. */
  public void retried(String type) {
    Counter.builder("jobqueue.jobs.retried")
        .description("Jobs returned to the queue for a retry")
        .tag("type", type)
        .register(registry)
        .increment();
  }

  /** A job entering the dead-letter queue. */
  public void dead(String type, String reason) {
    Counter.builder("jobqueue.jobs.dead")
        .description("Jobs moved to the dead-letter queue")
        .tag("type", type)
        .tag("reason", reason)
        .register(registry)
        .increment();
  }

  /** Wall time of the handler call. */
  public void executionTime(String type, String outcome, Duration elapsed) {
    Timer.builder("jobqueue.job.execution")
        .description("Handler execution duration")
        .tag("type", type)
        .tag("outcome", outcome)
        .publishPercentileHistogram()
        .minimumExpectedValue(MIN)
        .maximumExpectedValue(MAX)
        .register(registry)
        .record(elapsed);
  }

  /**
   * How long a job waited, runnable, before a worker claimed it: {@code claim time - run_at}, both
   * from the database clock. Measured from {@code run_at} (not creation) so delayed and retried
   * jobs report queueing delay rather than their intentional wait.
   */
  public void enqueueToStart(String type, Duration waited) {
    Timer.builder("jobqueue.job.enqueue.to.start")
        .description("Time a runnable job waited before being claimed")
        .tag("type", type)
        .publishPercentileHistogram()
        .minimumExpectedValue(MIN)
        .maximumExpectedValue(MAX)
        .register(registry)
        .record(waited.isNegative() ? Duration.ZERO : waited);
  }

  /** {@code created=false} means an idempotency key returned an existing job. */
  public void enqueued(String type, boolean created) {
    Counter.builder(created ? "jobqueue.jobs.enqueued" : "jobqueue.jobs.enqueue.duplicates")
        .description(
            created ? "Jobs accepted by the API" : "Enqueue requests answered with an existing job")
        .tag("type", type)
        .register(registry)
        .increment();
  }

  /** The reaper reclaimed a job whose lease expired; result is "requeued" or "dead". */
  public void leaseReclaimed(String type, String result) {
    Counter.builder("jobqueue.leases.reclaimed")
        .description("Jobs reclaimed after their lease expired")
        .tag("type", type)
        .tag("result", result)
        .register(registry)
        .increment();
  }

  /** A write was rejected by the fence: a zombie tried to report on a job it no longer owns. */
  public void fenced(String operation) {
    Counter.builder("jobqueue.fenced")
        .description("Stale-owner writes rejected by fencing")
        .tag("operation", operation)
        .register(registry)
        .increment();
  }

  /** A heartbeat found a lease it believed it held had been lost. */
  public void heartbeatLost() {
    Counter.builder("jobqueue.heartbeat.lost")
        .description("Leases found lost during heartbeat")
        .register(registry)
        .increment();
  }
}
