package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Regression test for a throughput bug the load test found: a saturated worker used to refill its
 * slots only once per poll interval. With a deliberately slow 500 ms poll interval and 8 slots the
 * old loop could start at most 16 jobs per second (about 6 s for 100 jobs); a worker that wakes as
 * soon as a slot frees needs a fraction of that.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "jobqueue.roles=api,worker",
      "jobqueue.worker.concurrency=8",
      "jobqueue.worker.batch-size=10",
      "jobqueue.worker.poll-interval=500ms",
      "jobqueue.worker.max-poll-interval=500ms"
    })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SlotRefillTest extends PostgresTestBase {

  @Autowired JdbcTemplate jdbc;

  @Test
  void aSaturatedWorkerRefillsSlotsImmediatelyInsteadOfOncePerPollInterval() {
    jdbc.update("DELETE FROM jobs");
    int total = 100;
    jdbc.update(
        "INSERT INTO jobs (type, payload) SELECT 'test-probe', '{}'::jsonb FROM generate_series(1, ?)",
        total);

    long start = System.nanoTime();
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                jdbc.queryForObject(
                        "SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'", Integer.class)
                    == total);
    double seconds = (System.nanoTime() - start) / 1e9;

    // 100 jobs x ~10 ms over 8 slots is well under a second of work. The old loop needed >= 6 s.
    assertThat(seconds).as("seconds to drain 100 jobs").isLessThan(3.0);
  }
}
