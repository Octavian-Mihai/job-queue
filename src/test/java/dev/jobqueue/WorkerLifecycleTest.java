package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jobqueue.core.JobService;
import dev.jobqueue.core.NewJob;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Crash recovery, heartbeats and lost-lease handling with the real worker, heartbeat and reaper.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "jobqueue.roles=api,worker",
      "jobqueue.worker.concurrency=8",
      "jobqueue.worker.poll-interval=20ms",
      "jobqueue.worker.max-poll-interval=100ms",
      "jobqueue.worker.lease-duration=1s",
      "jobqueue.worker.heartbeat-interval=250ms",
      "jobqueue.worker.reaper-interval=200ms",
      "jobqueue.retry.defaults.base-delay=20ms",
      "jobqueue.retry.defaults.max-delay=100ms",
      "jobqueue.retry.types.generate-report.base-delay=20ms",
      "jobqueue.retry.types.test-stubborn.execution-timeout=300ms"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class WorkerLifecycleTest extends PostgresTestBase {

  @Autowired JobService jobs;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;
  @Autowired TestHandlers.Sleeper sleeper;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
    sleeper.reset();
  }

  private UUID enqueue(String type, Map<String, Object> payload, Integer maxAttempts) {
    return jobs.enqueue(
            new NewJob("default", type, json.valueToTree(payload), 0, maxAttempts, null, 0, null))
        .job()
        .id();
  }

  /** What a crashed worker leaves behind: a RUNNING job, an open attempt, a lease that runs out. */
  private UUID crashedWorkerLeftBehind(int attempts, int maxAttempts) {
    UUID id =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, status, attempts, max_attempts, locked_by, lease_expires_at)"
                + " VALUES ('test-probe', 'RUNNING', ?, ?, 'dead-worker', now() + interval '1 s')"
                + " RETURNING id",
            UUID.class,
            attempts,
            maxAttempts);
    jdbc.update(
        "INSERT INTO job_attempts (job_id, attempt_number, worker_id) VALUES (?, ?, 'dead-worker')",
        id,
        attempts);
    return id;
  }

  private String status(UUID id) {
    return jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, id);
  }

  private void awaitStatus(UUID id, String expected) {
    await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals(expected));
  }

  private List<String> outcomes(UUID id) {
    return jdbc.queryForList(
        "SELECT coalesce(outcome, 'OPEN') FROM job_attempts WHERE job_id = ? ORDER BY attempt_number",
        String.class,
        id);
  }

  @Test
  void jobOfACrashedWorkerIsReclaimedAndCompletedByAnotherWorker() {
    UUID id = crashedWorkerLeftBehind(1, 3);

    awaitStatus(id, "SUCCEEDED");

    assertThat(outcomes(id)).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
    assertThat(
            jdbc.queryForList(
                "SELECT worker_id FROM job_attempts WHERE job_id = ? ORDER BY attempt_number",
                String.class,
                id))
        .hasSize(2)
        .first()
        .isEqualTo("dead-worker");
    assertThat(jdbc.queryForObject("SELECT attempts FROM jobs WHERE id = ?", Integer.class, id))
        .isEqualTo(2);
  }

  @Test
  void aJobThatKeepsCrashingItsWorkersEndsInTheDlq() {
    UUID id = crashedWorkerLeftBehind(3, 3); // already on its last attempt

    awaitStatus(id, "DEAD");

    assertThat(
            jdbc.queryForObject(
                "SELECT reason FROM dead_letter_jobs WHERE job_id = ?", String.class, id))
        .isEqualTo("MAX_ATTEMPTS_EXCEEDED");
    assertThat(outcomes(id)).containsExactly("LEASE_EXPIRED");
  }

  @Test
  void heartbeatsKeepALongRunningJobFromBeingReclaimed() {
    // Runs 3x longer than its 1s lease: only heartbeats can keep it alive.
    UUID id = enqueue("test-sleeper", Map.of("sleepMs", 3_000), 3);

    awaitStatus(id, "SUCCEEDED");

    assertThat(outcomes(id)).containsExactly("SUCCEEDED"); // never reclaimed, never retried
    assertThat(jdbc.queryForObject("SELECT attempts FROM jobs WHERE id = ?", Integer.class, id))
        .isEqualTo(1);
  }

  @Test
  void aHandlerThatLostItsLeaseIsInterruptedAndTheJobRunsAgain() {
    UUID id = enqueue("test-sleeper", Map.of("sleepMs", 30_000, "firstAttemptOnly", true), 3);
    String firstAttempt = TestHandlers.Sleeper.key(id, 1);
    await().atMost(Duration.ofSeconds(10)).until(() -> sleeper.started.contains(firstAttempt));

    // Take the job away exactly as the reaper would (e.g. this worker was paused past its lease).
    jdbc.update(
        "UPDATE jobs SET status='PENDING', locked_by=NULL, lease_expires_at=NULL, run_at=now()"
            + " WHERE id = ?",
        id);

    // The next heartbeat notices the lost lease and interrupts the now-pointless handler...
    await().atMost(Duration.ofSeconds(10)).until(() -> sleeper.interrupted.contains(firstAttempt));
    // ...and the job completes on its next attempt.
    awaitStatus(id, "SUCCEEDED");
    assertThat(jdbc.queryForObject("SELECT attempts FROM jobs WHERE id = ?", Integer.class, id))
        .isEqualTo(2);
  }

  @Test
  void aTimedOutHandlerThatIgnoresInterruptionStopsBeingHeartbeatedAndIsReclaimed() {
    // Attempt 1 ignores interrupts for 4s. Its 300ms timeout fires, heartbeats stop, the 1s lease
    // runs out and the reaper requeues the job well before the stubborn handler would return.
    UUID id = enqueue("test-stubborn", Map.of("stubbornMs", 4_000), 3);

    awaitStatus(id, "SUCCEEDED");

    assertThat(outcomes(id)).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
  }
}
