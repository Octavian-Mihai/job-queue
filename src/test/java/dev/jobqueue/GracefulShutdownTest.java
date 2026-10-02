package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jobqueue.core.JobService;
import dev.jobqueue.core.NewJob;
import dev.jobqueue.worker.JobClaimRepository;
import dev.jobqueue.worker.WorkerLoop;
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

/** stop() on a worker with jobs in flight: nothing may be lost. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "jobqueue.roles=api,worker",
      "jobqueue.worker.concurrency=8",
      "jobqueue.worker.poll-interval=20ms",
      "jobqueue.worker.max-poll-interval=100ms",
      "jobqueue.worker.lease-duration=30s",
      "jobqueue.worker.shutdown-grace-period=1500ms"
    })
@DirtiesContext(
    classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) // each test stops the worker
class GracefulShutdownTest extends PostgresTestBase {

  @Autowired WorkerLoop worker;
  @Autowired JobService jobs;
  @Autowired JobClaimRepository claims;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;
  @Autowired TestHandlers.Sleeper sleeper;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
    sleeper.reset();
  }

  private UUID enqueueSleeper(long sleepMs) {
    return jobs.enqueue(
            new NewJob(
                "default",
                "test-sleeper",
                json.valueToTree(Map.of("sleepMs", sleepMs)),
                0,
                3,
                null,
                0,
                null))
        .job()
        .id();
  }

  private int count(String status) {
    return jdbc.queryForObject("SELECT count(*) FROM jobs WHERE status = ?", Integer.class, status);
  }

  @Test
  void inFlightJobsFinishWithinTheGracePeriodAndNothingNewIsClaimed() {
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      ids.add(enqueueSleeper(500));
    }
    await().atMost(Duration.ofSeconds(10)).until(() -> sleeper.started.size() == 8);

    worker.stop(); // returns once the 8 in-flight jobs have finished

    assertThat(ids).allSatisfy(id -> assertThat(statusOf(id)).isEqualTo("SUCCEEDED"));
    assertThat(count("RUNNING")).isZero();
    // The worker has stopped claiming: a job enqueued afterwards stays PENDING.
    UUID later = enqueueSleeper(10);
    await()
        .during(Duration.ofMillis(600))
        .atMost(Duration.ofSeconds(3))
        .until(() -> statusOf(later).equals("PENDING"));
    assertThat(jdbc.queryForObject("SELECT attempts FROM jobs WHERE id = ?", Integer.class, later))
        .isZero();
  }

  @Test
  void jobsStillRunningAtTheDeadlineAreReleasedWithoutConsumingAnAttempt() {
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ids.add(enqueueSleeper(30_000));
    }
    await().atMost(Duration.ofSeconds(10)).until(() -> sleeper.started.size() == 3);

    long t0 = System.nanoTime();
    worker.stop(); // 1.5s grace, then release + interrupt
    long stopMillis = (System.nanoTime() - t0) / 1_000_000;

    assertThat(stopMillis).isLessThan(10_000);
    for (UUID id : ids) {
      var row = jdbc.queryForMap("SELECT * FROM jobs WHERE id = ?", id);
      assertThat(row.get("status")).isEqualTo("PENDING");
      assertThat(row.get("attempts")).isEqualTo(0); // the shutdown did not burn an attempt
      assertThat(row.get("locked_by")).isNull();
      assertThat(row.get("lease_expires_at")).isNull();
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id))
          .isZero();
    }
    assertThat(sleeper.interrupted).hasSize(3); // handlers were told to stop
    assertThat(jdbc.queryForObject("SELECT count(*) FROM dead_letter_jobs", Integer.class))
        .isZero();

    // Another worker can pick every one of them up immediately, as a clean first attempt.
    var claimed = claims.claim("other-worker", 10, Duration.ofSeconds(30), List.of());
    assertThat(claimed).extracting(j -> j.id()).containsExactlyInAnyOrderElementsOf(ids);
    assertThat(claimed).allSatisfy(j -> assertThat(j.attempts()).isEqualTo(1));
  }

  @Test
  void stopIsIdempotent() {
    worker.stop();
    worker.stop();
    assertThat(worker.isRunning()).isFalse();
  }

  private String statusOf(UUID id) {
    return jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, id);
  }
}
