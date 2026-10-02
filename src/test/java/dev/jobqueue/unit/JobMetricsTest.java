package dev.jobqueue.unit;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jobqueue.metrics.JobMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class JobMetricsTest {

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final JobMetrics metrics = new JobMetrics(registry);

  @Test
  void countersAreSplitByTypeAndOutcome() {
    metrics.attempt("send-email", "SUCCEEDED");
    metrics.attempt("send-email", "SUCCEEDED");
    metrics.attempt("send-email", "FAILED_RETRYABLE");
    metrics.attempt("deliver-webhook", "SUCCEEDED");

    assertThat(count("jobqueue.attempts", "type", "send-email", "outcome", "SUCCEEDED"))
        .isEqualTo(2);
    assertThat(count("jobqueue.attempts", "type", "send-email", "outcome", "FAILED_RETRYABLE"))
        .isEqualTo(1);
    assertThat(count("jobqueue.attempts", "type", "deliver-webhook", "outcome", "SUCCEEDED"))
        .isEqualTo(1);
  }

  @Test
  void retryDeadEnqueueAndRecoveryCounters() {
    metrics.retried("t");
    metrics.dead("t", "NON_RETRYABLE");
    metrics.enqueued("t", true);
    metrics.enqueued("t", false);
    metrics.enqueued("t", false);
    metrics.leaseReclaimed("t", "requeued");
    metrics.fenced("complete");
    metrics.heartbeatLost();

    assertThat(count("jobqueue.jobs.retried", "type", "t")).isEqualTo(1);
    assertThat(count("jobqueue.jobs.dead", "type", "t", "reason", "NON_RETRYABLE")).isEqualTo(1);
    assertThat(count("jobqueue.jobs.enqueued", "type", "t")).isEqualTo(1);
    assertThat(count("jobqueue.jobs.enqueue.duplicates", "type", "t")).isEqualTo(2);
    assertThat(count("jobqueue.leases.reclaimed", "type", "t", "result", "requeued")).isEqualTo(1);
    assertThat(count("jobqueue.fenced", "operation", "complete")).isEqualTo(1);
    assertThat(registry.get("jobqueue.heartbeat.lost").counter().count()).isEqualTo(1);
  }

  @Test
  void executionAndWaitTimersRecordDurations() {
    metrics.executionTime("t", "succeeded", Duration.ofMillis(250));
    metrics.executionTime("t", "succeeded", Duration.ofMillis(750));
    metrics.enqueueToStart("t", Duration.ofSeconds(2));

    var exec = registry.get("jobqueue.job.execution").tag("outcome", "succeeded").timer();
    assertThat(exec.count()).isEqualTo(2);
    assertThat(exec.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(1000);
    assertThat(registry.get("jobqueue.job.enqueue.to.start").timer().count()).isEqualTo(1);
  }

  @Test
  void negativeWaitFromClockQuirksIsClampedToZero() {
    metrics.enqueueToStart("t", Duration.ofMillis(-5));
    var timer = registry.get("jobqueue.job.enqueue.to.start").timer();
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isZero();
  }

  private double count(String name, String... tags) {
    return registry.get(name).tags(tags).counter().count();
  }
}
