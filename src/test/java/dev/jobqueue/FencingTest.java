package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jobqueue.core.JobStatus;
import dev.jobqueue.worker.JobClaimRepository;
import dev.jobqueue.worker.LeaseRepository;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Fencing: a "zombie" worker that lost its lease must not be able to overwrite the new owner's
 * result. The zombie here is a worker that was paused (GC, network partition, laptop lid) past its
 * lease, then wakes up and tries to report.
 */
class FencingTest extends PostgresTestBase {

  private static final Duration LEASE = Duration.ofSeconds(30);

  @Autowired JobClaimRepository claims;
  @Autowired LeaseRepository leases;
  @Autowired JdbcTemplate jdbc;

  private UUID jobId;

  @BeforeEach
  void newJob() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
    jobId =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, max_attempts, run_at)"
                + " VALUES ('send-email', 5, now() - interval '1 s') RETURNING id",
            UUID.class);
  }

  private void expireLeaseAndReap() {
    jdbc.update("UPDATE jobs SET lease_expires_at = now() - interval '1 s' WHERE id = ?", jobId);
    assertThat(leases.reapExpired(10)).hasSize(1);
  }

  private String status() {
    return jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, jobId);
  }

  private String lockedBy() {
    return jdbc.queryForObject("SELECT locked_by FROM jobs WHERE id = ?", String.class, jobId);
  }

  private List<String> outcomes() {
    return jdbc.queryForList(
        "SELECT coalesce(outcome, 'OPEN') FROM job_attempts WHERE job_id = ? ORDER BY attempt_number",
        String.class,
        jobId);
  }

  @Test
  void zombieCannotCompleteOrFailAJobThatWasReclaimedByAnotherWorker() {
    var zombieClaim = claims.claim("zombie", 1, LEASE, List.of()).get(0);
    expireLeaseAndReap(); // zombie is presumed dead; job is requeued
    var newOwnerClaim = claims.claim("new-owner", 1, LEASE, List.of()).get(0);
    assertThat(newOwnerClaim.attempts()).isEqualTo(2);

    // The zombie wakes up and tries to report its (old) result.
    assertThat(claims.complete(jobId, "zombie", zombieClaim.attempts())).isFalse();
    assertThat(
            claims.fail(
                jobId,
                "zombie",
                zombieClaim.attempts(),
                false,
                Duration.ZERO,
                "FAILED_NON_RETRYABLE",
                "zombie says boom",
                null))
        .isEmpty();

    // The new owner's claim is untouched.
    assertThat(status()).isEqualTo("RUNNING");
    assertThat(lockedBy()).isEqualTo("new-owner");
    assertThat(jdbc.queryForObject("SELECT attempts FROM jobs WHERE id = ?", Integer.class, jobId))
        .isEqualTo(2);
    assertThat(jdbc.queryForObject("SELECT last_error FROM jobs WHERE id = ?", String.class, jobId))
        .doesNotContain("zombie says boom");
    assertThat(jdbc.queryForObject("SELECT count(*) FROM dead_letter_jobs", Integer.class))
        .isZero();

    // The new owner finishes normally, and a late zombie report still cannot disturb it.
    assertThat(claims.complete(jobId, "new-owner", newOwnerClaim.attempts())).isTrue();
    assertThat(claims.complete(jobId, "zombie", zombieClaim.attempts())).isFalse();
    assertThat(status()).isEqualTo(JobStatus.SUCCEEDED.name());
    // The history tells the truth: attempt 1 died with its lease, attempt 2 succeeded.
    assertThat(outcomes()).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
  }

  @Test
  void attemptNumberFencesAStaleReportEvenFromTheSameWorkerId() {
    // The same worker id can claim the job again (e.g. a restarted process reusing its id).
    var first = claims.claim("w", 1, LEASE, List.of()).get(0);
    expireLeaseAndReap();
    var second = claims.claim("w", 1, LEASE, List.of()).get(0);
    assertThat(first.attempts()).isEqualTo(1);
    assertThat(second.attempts()).isEqualTo(2);

    assertThat(claims.complete(jobId, "w", first.attempts()))
        .isFalse(); // locked_by matches, attempt does not
    assertThat(status()).isEqualTo("RUNNING");
    assertThat(claims.complete(jobId, "w", second.attempts())).isTrue();
  }

  @Test
  void completionAfterTheReaperAlreadyRequeuedIsRejected() {
    var claim = claims.claim("slow", 1, LEASE, List.of()).get(0);
    expireLeaseAndReap();
    assertThat(status()).isEqualTo("PENDING");

    assertThat(claims.complete(jobId, "slow", claim.attempts())).isFalse();
    assertThat(status()).isEqualTo("PENDING"); // not resurrected as SUCCEEDED
  }

  @Test
  void afterTheJobIsFinishedNoOneCanChangeItsOutcome() {
    var claim = claims.claim("w", 1, LEASE, List.of()).get(0);
    assertThat(claims.complete(jobId, "w", claim.attempts())).isTrue();

    assertThat(claims.complete(jobId, "w", claim.attempts())).isFalse();
    assertThat(
            claims.fail(
                jobId,
                "w",
                claim.attempts(),
                false,
                Duration.ZERO,
                "FAILED_NON_RETRYABLE",
                "late",
                null))
        .isEmpty();
    assertThat(status()).isEqualTo("SUCCEEDED");
    assertThat(outcomes()).containsExactly("SUCCEEDED");
  }
}
