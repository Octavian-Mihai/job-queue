package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jobqueue.core.DeadLetter;
import dev.jobqueue.core.DeadLetterFilter;
import dev.jobqueue.core.DeadLetterService;
import dev.jobqueue.core.JobService;
import dev.jobqueue.core.NewJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/** The real worker loop against real Postgres. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "jobqueue.roles=api,worker",
      "jobqueue.worker.concurrency=8",
      "jobqueue.worker.batch-size=8",
      "jobqueue.worker.poll-interval=20ms",
      "jobqueue.worker.max-poll-interval=100ms",
      "jobqueue.retry.defaults.base-delay=20ms",
      "jobqueue.retry.defaults.max-delay=100ms",
      "jobqueue.retry.types.generate-report.base-delay=20ms",
      "jobqueue.retry.types.test-hang.execution-timeout=200ms",
      "jobqueue.retry.types.test-hang.max-attempts=2"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS) // stop the poller afterwards
class WorkerLoopTest extends PostgresTestBase {

  @Autowired JobService jobs;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;
  @Autowired TestHandlers.Probe probe;
  @Autowired TestHandlers.Gate gate;
  @Autowired DeadLetterService dlq;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
    jdbc.update("DELETE FROM email_outbox");
    probe.reset();
    gate.open.set(false);
  }

  private UUID enqueue(String type, Map<String, Object> payload, Integer maxAttempts) {
    return jobs.enqueue(
            new NewJob("default", type, json.valueToTree(payload), 0, maxAttempts, null, 0, null))
        .job()
        .id();
  }

  private String status(UUID id) {
    return jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, id);
  }

  private void awaitStatus(UUID id, String expected) {
    await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals(expected));
  }

  private List<String> outcomes(UUID id) {
    return jdbc.queryForList(
        "SELECT outcome FROM job_attempts WHERE job_id = ? ORDER BY attempt_number",
        String.class,
        id);
  }

  @Test
  void runsManyJobsConcurrentlyExactlyOnceEachWithinTheConcurrencyBound() {
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      ids.add(enqueue("test-probe", Map.of(), 3));
    }

    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'", Integer.class)
                    == 200);

    assertThat(probe.executions).hasSize(200);
    assertThat(probe.executions.values()).allSatisfy(n -> assertThat(n.get()).isEqualTo(1));
    assertThat(probe.overlaps.get()).isZero();
    assertThat(probe.peakParallel.get()).isGreaterThan(1).isLessThanOrEqualTo(8);
    assertThat(ids).allSatisfy(id -> assertThat(outcomes(id)).containsExactly("SUCCEEDED"));
  }

  @Test
  void retryableFailureIsRetriedUntilItSucceeds() {
    UUID id = enqueue("test-flaky", Map.of("failFirst", 2), 5);

    awaitStatus(id, "SUCCEEDED");

    assertThat(outcomes(id)).containsExactly("FAILED_RETRYABLE", "FAILED_RETRYABLE", "SUCCEEDED");
    assertThat(jdbc.queryForObject("SELECT attempts FROM jobs WHERE id = ?", Integer.class, id))
        .isEqualTo(3);
  }

  @Test
  void unknownExceptionsAreTreatedAsRetryable() {
    UUID id = enqueue("test-flaky", Map.of("failFirst", 1, "kind", "unknown"), 3);

    awaitStatus(id, "SUCCEEDED");

    assertThat(outcomes(id)).containsExactly("FAILED_RETRYABLE", "SUCCEEDED");
  }

  @Test
  void nonRetryableFailureGoesStraightToDead() {
    UUID id = enqueue("test-flaky", Map.of("kind", "permanent"), 5);

    awaitStatus(id, "DEAD");

    assertThat(outcomes(id)).containsExactly("FAILED_NON_RETRYABLE");
    assertThat(jdbc.queryForObject("SELECT last_error FROM jobs WHERE id = ?", String.class, id))
        .contains("never retry");
    assertThat(
            jdbc.queryForObject(
                "SELECT stack_trace FROM job_attempts WHERE job_id = ?", String.class, id))
        .contains("NonRetryableException");
  }

  @Test
  void exhaustingAttemptsEndsDead() {
    UUID id = enqueue("test-flaky", Map.of("failFirst", 99), 2);

    awaitStatus(id, "DEAD");

    assertThat(outcomes(id)).containsExactly("FAILED_RETRYABLE", "FAILED_RETRYABLE");
    assertThat(
            jdbc.queryForObject(
                "SELECT finished_at IS NOT NULL FROM jobs WHERE id = ?", Boolean.class, id))
        .isTrue();
  }

  @Test
  void jobWithoutRegisteredHandlerIsDeadNotStuck() {
    // Bypasses the enqueue-time type check, as when a type is removed after jobs were queued.
    UUID id =
        jdbc.queryForObject(
            "INSERT INTO jobs (type) VALUES ('removed-type') RETURNING id", UUID.class);

    awaitStatus(id, "DEAD");

    assertThat(jdbc.queryForObject("SELECT last_error FROM jobs WHERE id = ?", String.class, id))
        .contains("no handler registered");
  }

  @Test
  void demoHandlersRunEndToEnd() {
    UUID email = enqueue("send-email", Map.of("to", "a@b.c", "subject", "hi", "simulate", "ok"), 3);
    UUID hook = enqueue("deliver-webhook", Map.of("url", "https://x.test", "simulate", "ok"), 3);
    UUID report = enqueue("generate-report", Map.of("sleepMs", 50, "cpuMs", 20), 3);
    UUID badHook =
        enqueue("deliver-webhook", Map.of("url", "https://x.test", "simulate", "permanent"), 3);

    awaitStatus(email, "SUCCEEDED");
    awaitStatus(hook, "SUCCEEDED");
    awaitStatus(report, "SUCCEEDED");
    awaitStatus(badHook, "DEAD");

    assertThat(jdbc.queryForObject("SELECT count(*) FROM email_outbox", Integer.class))
        .isEqualTo(1);
  }

  private DeadLetter deadLetterFor(UUID jobId) {
    return dlq.list(new DeadLetterFilter(null, null, null, null), 0, 100).items().stream()
        .filter(d -> d.jobId().equals(jobId))
        .findFirst()
        .orElseThrow();
  }

  @Test
  void exhaustedJobIsCopiedToDlqWithReasonAndFullAttemptHistory() {
    UUID id = enqueue("test-flaky", Map.of("failFirst", 99), 3);

    awaitStatus(id, "DEAD");

    DeadLetter entry = deadLetterFor(id);
    assertThat(entry.reason()).isEqualTo("MAX_ATTEMPTS_EXCEEDED");
    assertThat(entry.attempts()).isEqualTo(3);
    assertThat(entry.maxAttempts()).isEqualTo(3);
    assertThat(entry.lastError()).contains("transient on attempt 3");
    assertThat(entry.replayedAt()).isNull();
    var history = dlq.attempts(entry);
    assertThat(history).hasSize(3);
    assertThat(history).extracting(a -> a.outcome()).containsOnly("FAILED_RETRYABLE");
    assertThat(history).extracting(a -> a.attemptNumber()).containsExactly(1, 2, 3);
    assertThat(history).allSatisfy(a -> assertThat(a.errorMessage()).startsWith("transient"));
    assertThat(history.get(0).stackTrace()).contains("RetryableException");
  }

  @Test
  void nonRetryableJobIsDeadLetteredImmediatelyWithItsReason() {
    UUID id = enqueue("test-flaky", Map.of("kind", "permanent"), 5);

    awaitStatus(id, "DEAD");

    DeadLetter entry = deadLetterFor(id);
    assertThat(entry.reason()).isEqualTo("NON_RETRYABLE");
    assertThat(entry.attempts()).isEqualTo(1);
    assertThat(dlq.attempts(entry)).hasSize(1);
  }

  @Test
  void succeededJobsNeverReachTheDlq() {
    UUID id = enqueue("test-flaky", Map.of("failFirst", 1), 3);
    awaitStatus(id, "SUCCEEDED");
    assertThat(jdbc.queryForObject("SELECT count(*) FROM dead_letter_jobs", Integer.class))
        .isZero();
  }

  @Test
  void timedOutAttemptsAreInterruptedCountedAndRetried() {
    UUID id = enqueue("test-hang", Map.of(), null);

    awaitStatus(id, "DEAD"); // type override: max-attempts=2, timeout 200ms

    assertThat(outcomes(id)).containsExactly("TIMED_OUT", "TIMED_OUT");
    assertThat(jdbc.queryForObject("SELECT last_error FROM jobs WHERE id = ?", String.class, id))
        .contains("timed out after 200 ms");
    assertThat(deadLetterFor(id).reason()).isEqualTo("MAX_ATTEMPTS_EXCEEDED");
  }

  @Test
  void aTimedOutHandlerFreesItsWorkerSlot() {
    // 8 slots; 10 hanging jobs would deadlock the pool if timeouts did not release slots.
    for (int i = 0; i < 10; i++) {
      enqueue("test-hang", Map.of(), 1);
    }
    UUID ok = enqueue("test-probe", Map.of(), 3);

    awaitStatus(ok, "SUCCEEDED");
    await()
        .atMost(Duration.ofSeconds(20))
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT count(*) FROM jobs WHERE status = 'DEAD'", Integer.class)
                    == 10);
  }

  @Test
  void replayedJobGetsAFreshAttemptCycleAndCanSucceed() {
    UUID original = enqueue("test-gate", Map.of("note", "replay me"), 2);
    awaitStatus(original, "DEAD");
    DeadLetter entry = deadLetterFor(original);

    gate.open.set(true); // the downstream is fixed
    UUID replayed = dlq.replayOne(entry.id());

    awaitStatus(replayed, "SUCCEEDED");
    assertThat(replayed).isNotEqualTo(original);
    assertThat(outcomes(replayed)).containsExactly("SUCCEEDED"); // attempt cycle restarted at 1
    assertThat(status(original)).isEqualTo("DEAD"); // history is untouched
    DeadLetter after = dlq.get(entry.id());
    assertThat(after.replayedAt()).isNotNull();
    assertThat(after.replayedJobId()).isEqualTo(replayed);
    assertThat(
            jdbc.queryForObject(
                "SELECT payload->>'note' FROM jobs WHERE id = ?", String.class, replayed))
        .isEqualTo("replay me");
  }
}
