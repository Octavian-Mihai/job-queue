package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.jobqueue.config.JobQueueProperties;
import dev.jobqueue.config.Role;
import dev.jobqueue.handler.HandlerRegistry;
import dev.jobqueue.metrics.JobMetrics;
import dev.jobqueue.retry.RetryPolicies;
import dev.jobqueue.worker.InFlightJobs;
import dev.jobqueue.worker.JobClaimRepository;
import dev.jobqueue.worker.JobExecutor;
import dev.jobqueue.worker.LeaseRepository;
import dev.jobqueue.worker.Reaper;
import dev.jobqueue.worker.WorkerLoop;
import dev.jobqueue.worker.WorkerProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Eight independent workers (each with its own id, in-flight registry, poller, heartbeat) hammering
 * one database. They are separate {@link WorkerLoop} instances built by hand because a Spring
 * context only holds one; everything else is the production code.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "jobqueue.roles=api",
      "spring.datasource.hikari.maximum-pool-size=40",
      "jobqueue.retry.defaults.base-delay=20ms",
      "jobqueue.retry.defaults.max-delay=100ms",
      "jobqueue.retry.types.generate-report.base-delay=20ms"
    })
class MultiWorkerTest extends PostgresTestBase {

  private static final int WORKERS = 8;

  @Autowired HandlerRegistry registry;
  @Autowired JobClaimRepository claims;
  @Autowired LeaseRepository leases;
  @Autowired RetryPolicies policies;
  @Autowired JobMetrics metrics;
  @Autowired JdbcTemplate jdbc;
  @Autowired TestHandlers.Probe probe;

  private final List<WorkerLoop> loops = new ArrayList<>();

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
    probe.reset();
  }

  @AfterEach
  void stopWorkers() {
    loops.forEach(WorkerLoop::stop);
    loops.clear();
  }

  private void startWorkers() {
    for (int i = 0; i < WORKERS; i++) {
      var identity = new JobQueueProperties(Set.of(Role.WORKER), "worker-" + i);
      var props =
          new WorkerProperties(
              4,
              10,
              Duration.ofMillis(10),
              Duration.ofMillis(50),
              Duration.ofSeconds(20),
              null,
              Duration.ofMillis(500),
              0,
              Duration.ofSeconds(5),
              null);
      var inFlight = new InFlightJobs();
      var executor = new JobExecutor(registry, claims, policies, inFlight, metrics, identity);
      var loop = new WorkerLoop(claims, executor, leases, inFlight, metrics, props, identity);
      loops.add(loop);
      loop.start();
    }
  }

  private int count(String where) {
    return jdbc.queryForObject("SELECT count(*) FROM jobs WHERE " + where, Integer.class);
  }

  @Test
  void eightWorkersProcess5000JobsEachExactlyOnceNeverConcurrently() {
    int total = 5_000;
    jdbc.update(
        "INSERT INTO jobs (type, payload) SELECT 'test-probe', '{}'::jsonb FROM generate_series(1, ?)",
        total);

    startWorkers();
    await().atMost(Duration.ofSeconds(90)).until(() -> count("status = 'SUCCEEDED'") == total);

    // every job reached a terminal state, and it is the right one
    assertThat(count("status NOT IN ('SUCCEEDED')")).isZero();
    // no job ever executed concurrently with itself, nor more than once
    assertThat(probe.executions).hasSize(total);
    assertThat(probe.executions.values()).allSatisfy(n -> assertThat(n.get()).isEqualTo(1));
    assertThat(probe.overlaps.get()).isZero();
    // bookkeeping agrees: one closed attempt per job, owned by some worker
    assertThat(count("attempts <> 1")).isZero();
    assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class))
        .isEqualTo(total);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM job_attempts WHERE outcome IS DISTINCT FROM 'SUCCEEDED'",
                Integer.class))
        .isZero();
    assertThat(count("locked_by IS NOT NULL OR lease_expires_at IS NOT NULL")).isZero();
    // and the work really was spread over the pool, within the configured parallelism
    assertThat(
            jdbc.queryForObject(
                "SELECT count(DISTINCT worker_id) FROM job_attempts", Integer.class))
        .isEqualTo(WORKERS);
    assertThat(probe.peakParallel.get()).isGreaterThan(4).isLessThanOrEqualTo(WORKERS * 4);
  }

  @Test
  void mixedOutcomesUnderEightWorkersLoseNothingAndLeaveNoOpenAttempts() {
    int ok = 600;
    int flaky = 300; // fail once, then succeed
    int doomed = 100; // non-retryable: straight to the DLQ
    jdbc.update(
        "INSERT INTO jobs (type, payload) SELECT 'test-probe', '{}'::jsonb FROM generate_series(1, ?)",
        ok);
    jdbc.update(
        "INSERT INTO jobs (type, payload) SELECT 'test-flaky', '{\"failFirst\":1}'::jsonb"
            + " FROM generate_series(1, ?)",
        flaky);
    jdbc.update(
        "INSERT INTO jobs (type, payload) SELECT 'test-flaky', '{\"kind\":\"permanent\"}'::jsonb"
            + " FROM generate_series(1, ?)",
        doomed);

    startWorkers();
    await()
        .atMost(Duration.ofSeconds(90))
        .until(() -> count("status IN ('SUCCEEDED', 'DEAD')") == ok + flaky + doomed);

    assertThat(count("status = 'SUCCEEDED'")).isEqualTo(ok + flaky);
    assertThat(count("status = 'DEAD'")).isEqualTo(doomed);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM dead_letter_jobs", Integer.class))
        .isEqualTo(doomed);
    // exactly the right number of attempts: 1 for ok and doomed, 2 for flaky
    assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class))
        .isEqualTo(ok + doomed + 2 * flaky);
    // no attempt of a finished job was left open, and the counters match the rows
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM job_attempts WHERE outcome IS NULL", Integer.class))
        .isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM jobs j WHERE j.attempts <>"
                    + " (SELECT count(*) FROM job_attempts a WHERE a.job_id = j.id)",
                Integer.class))
        .isZero();
  }

  @Test
  void aReaperRunningAlongsideHealthyWorkersNeverStealsALiveJob() {
    // Short lease + slow-ish jobs: only working heartbeats keep jobs from being reclaimed.
    int total = 200;
    jdbc.update(
        "INSERT INTO jobs (type, payload) SELECT 'test-sleeper', '{\"sleepMs\":1500}'::jsonb"
            + " FROM generate_series(1, ?)",
        total);
    var props =
        new WorkerProperties(
            4,
            10,
            Duration.ofMillis(10),
            Duration.ofMillis(50),
            Duration.ofSeconds(1),
            Duration.ofMillis(250),
            Duration.ofMillis(100),
            0,
            Duration.ofSeconds(5),
            null);
    var reaper = new Reaper(leases, props, metrics);
    for (int i = 0; i < 4; i++) {
      var identity = new JobQueueProperties(Set.of(Role.WORKER), "hb-worker-" + i);
      var inFlight = new InFlightJobs();
      var executor = new JobExecutor(registry, claims, policies, inFlight, metrics, identity);
      var loop = new WorkerLoop(claims, executor, leases, inFlight, metrics, props, identity);
      loops.add(loop);
      loop.start();
    }
    reaper.start();
    try {
      await().atMost(Duration.ofSeconds(90)).until(() -> count("status = 'SUCCEEDED'") == total);
    } finally {
      reaper.stop();
    }

    assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class))
        .isEqualTo(total); // no job was ever reclaimed and re-run
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM job_attempts WHERE outcome = 'LEASE_EXPIRED'", Integer.class))
        .isZero();
  }
}
