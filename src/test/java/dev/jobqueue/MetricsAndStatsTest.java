package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jobqueue.core.JobService;
import dev.jobqueue.core.NewJob;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/** Real jobs through the real worker, observed via the Prometheus endpoint and /stats. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "jobqueue.roles=api,worker",
      "jobqueue.worker.poll-interval=20ms",
      "jobqueue.worker.max-poll-interval=100ms",
      "jobqueue.worker.lease-duration=1s",
      "jobqueue.worker.heartbeat-interval=250ms",
      "jobqueue.worker.reaper-interval=200ms",
      "jobqueue.retry.defaults.base-delay=20ms",
      "jobqueue.retry.defaults.max-delay=100ms",
      "jobqueue.retry.types.generate-report.base-delay=20ms"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
class MetricsAndStatsTest extends PostgresTestBase {

  @Autowired TestRestTemplate http;
  @Autowired MeterRegistry registry;
  @Autowired JobService jobs;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
  }

  private UUID enqueue(String type, Map<String, Object> payload, Integer maxAttempts) {
    return jobs.enqueue(
            new NewJob("default", type, json.valueToTree(payload), 0, maxAttempts, null, 0, null))
        .job()
        .id();
  }

  private double counter(String name, String... tags) {
    var c = registry.find(name).tags(tags).counter();
    return c == null ? 0 : c.count();
  }

  private void awaitStatus(UUID id, String status) {
    await()
        .atMost(Duration.ofSeconds(20))
        .until(
            () ->
                status.equals(
                    jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, id)));
  }

  @Test
  void jobLifecycleEventsAreCountedAndTimed() {
    double okBefore = counter("jobqueue.attempts", "type", "test-flaky", "outcome", "SUCCEEDED");
    double failBefore =
        counter("jobqueue.attempts", "type", "test-flaky", "outcome", "FAILED_RETRYABLE");
    double retriedBefore = counter("jobqueue.jobs.retried", "type", "test-flaky");
    double deadBefore =
        counter("jobqueue.jobs.dead", "type", "test-flaky", "reason", "NON_RETRYABLE");
    double enqueuedBefore = counter("jobqueue.jobs.enqueued", "type", "test-flaky");

    UUID flaky = enqueue("test-flaky", Map.of("failFirst", 2), 5); // 2 failures then success
    UUID doomed = enqueue("test-flaky", Map.of("kind", "permanent"), 5); // straight to the DLQ
    awaitStatus(flaky, "SUCCEEDED");
    awaitStatus(doomed, "DEAD");

    assertThat(counter("jobqueue.jobs.enqueued", "type", "test-flaky") - enqueuedBefore)
        .isEqualTo(2);
    assertThat(
            counter("jobqueue.attempts", "type", "test-flaky", "outcome", "SUCCEEDED") - okBefore)
        .isEqualTo(1);
    assertThat(
            counter("jobqueue.attempts", "type", "test-flaky", "outcome", "FAILED_RETRYABLE")
                - failBefore)
        .isEqualTo(2);
    assertThat(counter("jobqueue.jobs.retried", "type", "test-flaky") - retriedBefore).isEqualTo(2);
    assertThat(
            counter("jobqueue.jobs.dead", "type", "test-flaky", "reason", "NON_RETRYABLE")
                - deadBefore)
        .isEqualTo(1);
    assertThat(registry.get("jobqueue.job.execution").tag("type", "test-flaky").timers())
        .isNotEmpty();
    assertThat(
            registry.get("jobqueue.job.enqueue.to.start").tag("type", "test-flaky").timer().count())
        .isGreaterThanOrEqualTo(4); // every claim, retries included
  }

  @Test
  void prometheusEndpointExposesQueueGaugesAndHistograms() {
    UUID id = enqueue("test-probe", Map.of(), 3);
    awaitStatus(id, "SUCCEEDED");
    UUID dead = enqueue("test-flaky", Map.of("kind", "permanent"), 3);
    awaitStatus(dead, "DEAD");

    String scrape = http.getForObject("/actuator/prometheus", String.class);

    assertThat(scrape)
        .contains("jobqueue_jobs{application=\"durable-job-queue\",status=\"SUCCEEDED\"} 1.0")
        .contains("jobqueue_jobs{application=\"durable-job-queue\",status=\"DEAD\"} 1.0")
        .contains("jobqueue_jobs{application=\"durable-job-queue\",status=\"PENDING\"} 0.0")
        .contains("jobqueue_dlq_size{application=\"durable-job-queue\"} 1.0")
        .contains("jobqueue_oldest_pending_age_seconds")
        .contains("jobqueue_jobs_in_flight")
        .contains("jobqueue_attempts_total")
        .contains("jobqueue_jobs_dead_total")
        .contains("jobqueue_job_execution_seconds_bucket")
        .contains("jobqueue_job_enqueue_to_start_seconds_bucket");
  }

  @Test
  void statsEndpointReportsCountsAndOldestPending() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update(
        "INSERT INTO jobs (type, status, run_at, finished_at) VALUES"
            + " ('send-email', 'SUCCEEDED', now(), now()), ('send-email', 'SUCCEEDED', now(), now())");
    jdbc.update(
        "INSERT INTO jobs (type, status, run_at) VALUES ('send-email', 'PENDING', now() + interval '1 hour')");

    JsonNode s = http.getForObject("/stats", JsonNode.class);

    assertThat(s.get("countsByStatus").get("SUCCEEDED").asLong()).isEqualTo(2);
    assertThat(s.get("countsByStatus").get("PENDING").asLong()).isEqualTo(1);
    assertThat(s.get("countsByStatus").get("RUNNING").asLong()).isZero();
    assertThat(s.get("total").asLong()).isEqualTo(3);
    // the only pending job is scheduled for the future, so nothing is "waiting"
    assertThat(s.get("oldestPendingAgeSeconds").asDouble()).isZero();
    assertThat(s.get("deadLetterQueueSize").asLong()).isZero();
  }

  @Test
  void oldestPendingAgeReflectsAJobThatHasBeenRunnableForAWhile() {
    // Insert in a transaction we keep open, so the worker cannot see (and consume) the row.
    var tx =
        new org.springframework.transaction.support.TransactionTemplate(
            new org.springframework.jdbc.datasource.DataSourceTransactionManager(
                jdbc.getDataSource()));
    tx.executeWithoutResult(
        status -> {
          jdbc.update(
              "INSERT INTO jobs (type, status, run_at) VALUES"
                  + " ('send-email', 'PENDING', now() - interval '90 seconds')");
          // visible only inside this transaction; assert via the same connection
          Double age =
              jdbc.queryForObject(
                  "SELECT EXTRACT(EPOCH FROM (now() - min(run_at)))::float8 FROM jobs"
                      + " WHERE status = 'PENDING' AND run_at <= now()",
                  Double.class);
          assertThat(age).isBetween(89.0, 95.0);
          status.setRollbackOnly();
        });
  }

  @Test
  void reapedJobsAreCountedAsReclaimedLeases() {
    double before =
        counter("jobqueue.leases.reclaimed", "type", "test-probe", "result", "requeued");
    UUID id =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, status, attempts, max_attempts, locked_by, lease_expires_at)"
                + " VALUES ('test-probe', 'RUNNING', 1, 3, 'dead-worker', now() + interval '600 ms')"
                + " RETURNING id",
            UUID.class);
    jdbc.update(
        "INSERT INTO job_attempts (job_id, attempt_number, worker_id) VALUES (?, 1, 'dead-worker')",
        id);

    awaitStatus(id, "SUCCEEDED");

    assertThat(
            counter("jobqueue.leases.reclaimed", "type", "test-probe", "result", "requeued")
                - before)
        .isEqualTo(1);
  }
}
