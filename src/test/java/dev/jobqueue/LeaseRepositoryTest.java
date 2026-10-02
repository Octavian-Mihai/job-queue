package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jobqueue.worker.JobClaimRepository;
import dev.jobqueue.worker.Lease;
import dev.jobqueue.worker.LeaseRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/** Heartbeat, reaper and release SQL in isolation (roles=api: no worker is competing). */
class LeaseRepositoryTest extends PostgresTestBase {

  private static final Duration LEASE = Duration.ofSeconds(30);

  @Autowired LeaseRepository leases;
  @Autowired JobClaimRepository claims;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
  }

  /** A job already RUNNING for a (dead) worker, with its attempt row. */
  private UUID runningFor(String worker, int attempts, int maxAttempts, String leaseOffset) {
    UUID id =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, status, attempts, max_attempts, locked_by, lease_expires_at)"
                + " VALUES ('send-email', 'RUNNING', ?, ?, ?, now() + ?::interval) RETURNING id",
            UUID.class,
            attempts,
            maxAttempts,
            worker,
            leaseOffset);
    jdbc.update(
        "INSERT INTO job_attempts (job_id, attempt_number, worker_id) VALUES (?, ?, ?)",
        id,
        attempts,
        worker);
    return id;
  }

  private String status(UUID id) {
    return jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, id);
  }

  private List<String> outcomes(UUID id) {
    return jdbc.queryForList(
        "SELECT coalesce(outcome, 'OPEN') FROM job_attempts WHERE job_id = ? ORDER BY attempt_number",
        String.class,
        id);
  }

  // ---- heartbeat -----------------------------------------------------------------------

  @Test
  void extendPushesTheLeaseOutForOwnedClaimsOnly() {
    UUID mine = runningFor("w1", 1, 3, "5 seconds");
    UUID theirs = runningFor("w2", 1, 3, "5 seconds");
    UUID wrongAttempt = runningFor("w1", 2, 3, "5 seconds");

    Set<UUID> extended =
        leases.extend(
            "w1",
            List.of(new Lease(mine, 1), new Lease(theirs, 1), new Lease(wrongAttempt, 1)),
            LEASE);

    assertThat(extended).containsExactly(mine); // not-owner and stale-attempt claims are lost
    assertThat(
            jdbc.queryForObject(
                "SELECT lease_expires_at > now() + interval '25 s' FROM jobs WHERE id = ?",
                Boolean.class,
                mine))
        .isTrue();
    assertThat(
            jdbc.queryForObject(
                "SELECT lease_expires_at < now() + interval '10 s' FROM jobs WHERE id = ?",
                Boolean.class,
                theirs))
        .isTrue();
  }

  @Test
  void extendReportsClaimsThatAreNoLongerRunning() {
    UUID id = runningFor("w1", 1, 3, "5 seconds");
    jdbc.update(
        "UPDATE jobs SET status='SUCCEEDED', locked_by=NULL, lease_expires_at=NULL WHERE id=?", id);
    assertThat(leases.extend("w1", List.of(new Lease(id, 1)), LEASE)).isEmpty();
    assertThat(leases.extend("w1", List.of(), LEASE)).isEmpty();
  }

  // ---- reaper --------------------------------------------------------------------------

  @Test
  void reaperRequeuesExpiredJobsAndClosesTheirAttempt() {
    UUID expired = runningFor("dead-worker", 1, 3, "-1 seconds");
    UUID alive = runningFor("live-worker", 1, 3, "30 seconds");

    var reaped = leases.reapExpired(100);

    assertThat(reaped).extracting(r -> r.jobId()).containsExactly(expired);
    assertThat(status(expired)).isEqualTo("PENDING");
    assertThat(status(alive)).isEqualTo("RUNNING");
    var row = jdbc.queryForMap("SELECT * FROM jobs WHERE id = ?", expired);
    assertThat(row.get("locked_by")).isNull();
    assertThat(row.get("lease_expires_at")).isNull();
    assertThat(row.get("attempts")).isEqualTo(1); // a crash consumes the attempt
    assertThat(row.get("last_error").toString()).contains("lease expired");
    assertThat(outcomes(expired)).containsExactly("LEASE_EXPIRED");
    assertThat(
            jdbc.queryForObject(
                "SELECT error_message FROM job_attempts WHERE job_id = ?", String.class, expired))
        .contains("dead-worker");
    assertThat(jdbc.queryForObject("SELECT count(*) FROM dead_letter_jobs", Integer.class))
        .isZero();
  }

  @Test
  void reaperBuriesAJobThatKeepsKillingItsWorkers() {
    UUID poison = runningFor("dead-worker", 3, 3, "-1 seconds"); // last attempt, worker died

    var reaped = leases.reapExpired(100);

    assertThat(reaped).singleElement().satisfies(r -> assertThat(r.newStatus()).isEqualTo("DEAD"));
    assertThat(status(poison)).isEqualTo("DEAD");
    var dlq = jdbc.queryForMap("SELECT * FROM dead_letter_jobs WHERE job_id = ?", poison);
    assertThat(dlq.get("reason")).isEqualTo("MAX_ATTEMPTS_EXCEEDED");
    assertThat(dlq.get("last_error").toString()).contains("lease expired");
    assertThat(dlq.get("attempts")).isEqualTo(3);
    assertThat(
            jdbc.queryForObject(
                "SELECT finished_at IS NOT NULL FROM jobs WHERE id = ?", Boolean.class, poison))
        .isTrue();
    assertThat(outcomes(poison)).containsExactly("LEASE_EXPIRED");
  }

  @Test
  void reaperHonoursTheBatchLimit() {
    for (int i = 0; i < 5; i++) {
      runningFor("dead", 1, 3, "-1 minutes");
    }
    assertThat(leases.reapExpired(2)).hasSize(2);
    assertThat(leases.reapExpired(10)).hasSize(3);
    assertThat(leases.reapExpired(10)).isEmpty();
  }

  @Test
  void concurrentReapersReclaimEveryJobExactlyOnce() throws Exception {
    int total = 500;
    jdbc.update(
        "INSERT INTO jobs (type, status, attempts, max_attempts, locked_by, lease_expires_at)"
            + " SELECT 'send-email', 'RUNNING', 1, 3, 'dead', now() - interval '1 minute'"
            + " FROM generate_series(1, ?)",
        total);
    jdbc.update(
        "INSERT INTO job_attempts (job_id, attempt_number, worker_id) SELECT id, 1, 'dead' FROM jobs");
    int reapers = 8;
    ExecutorService pool = Executors.newFixedThreadPool(reapers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<List<UUID>>> futures = new ArrayList<>();
    for (int r = 0; r < reapers; r++) {
      futures.add(
          pool.submit(
              () -> {
                start.await();
                List<UUID> mine = new ArrayList<>();
                List<LeaseRepository.Reaped> batch;
                while (!(batch = leases.reapExpired(20)).isEmpty()) {
                  batch.forEach(b -> mine.add(b.jobId()));
                }
                return mine;
              }));
    }
    start.countDown();
    List<UUID> all = new ArrayList<>();
    for (var f : futures) {
      all.addAll(f.get());
    }
    pool.shutdown();

    assertThat(all).hasSize(total);
    assertThat(new HashSet<>(all)).hasSize(total);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE status = 'PENDING'", Integer.class))
        .isEqualTo(total);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM job_attempts WHERE outcome = 'LEASE_EXPIRED'", Integer.class))
        .isEqualTo(total);
  }

  // ---- graceful release ----------------------------------------------------------------

  @Test
  void releaseReturnsTheJobWithoutConsumingAnAttempt() {
    UUID id =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, run_at) VALUES ('send-email', now() - interval '1 s') RETURNING id",
            UUID.class);
    var job = claims.claim("w1", 1, LEASE, List.of()).get(0);
    assertThat(job.attempts()).isEqualTo(1);

    int released = leases.release("w1", List.of(new Lease(id, 1)));

    assertThat(released).isEqualTo(1);
    var row = jdbc.queryForMap("SELECT * FROM jobs WHERE id = ?", id);
    assertThat(row.get("status")).isEqualTo("PENDING");
    assertThat(row.get("attempts")).isEqualTo(0);
    assertThat(row.get("locked_by")).isNull();
    assertThat(outcomes(id)).isEmpty(); // the unfinished attempt row is removed
    // and the job is immediately claimable again, as a clean first attempt
    var again = claims.claim("w2", 1, LEASE, List.of()).get(0);
    assertThat(again.id()).isEqualTo(id);
    assertThat(again.attempts()).isEqualTo(1);
    assertThat(outcomes(id)).containsExactly("OPEN");
  }

  @Test
  void releaseIsFenced() {
    UUID id = runningFor("w1", 1, 3, "30 seconds");
    assertThat(leases.release("someone-else", List.of(new Lease(id, 1)))).isZero();
    assertThat(leases.release("w1", List.of(new Lease(id, 2)))).isZero();
    assertThat(status(id)).isEqualTo("RUNNING");
    assertThat(leases.release("w1", List.of())).isZero();
  }
}
