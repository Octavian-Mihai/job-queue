package dev.jobqueue.worker;

import dev.jobqueue.core.Job;
import dev.jobqueue.core.JobRowMapper;
import dev.jobqueue.core.JobStatus;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** The worker-side SQL: claiming jobs and recording how an attempt ended. */
@Repository
public class JobClaimRepository {

  private final NamedParameterJdbcTemplate jdbc;
  private final JobRowMapper jobMapper;

  public JobClaimRepository(NamedParameterJdbcTemplate jdbc, JobRowMapper jobMapper) {
    this.jdbc = jdbc;
    this.jobMapper = jobMapper;
  }

  /**
   * Atomically claims up to {@code limit} runnable jobs for {@code workerId}.
   *
   * <p><b>Why {@code FOR UPDATE SKIP LOCKED} prevents double-claiming:</b> the inner SELECT takes a
   * row lock on every row it returns. A second worker running the same statement at the same time
   * skips rows that are locked instead of waiting on them, so two workers can never both lock (and
   * therefore never both claim) the same row. The lock is held until the surrounding statement
   * commits, by which point the row is already {@code RUNNING} and no longer matches {@code
   * status='PENDING'}.
   *
   * <p><b>Why it avoids contention:</b> without SKIP LOCKED, N workers all want the same
   * highest-priority rows; they would queue on one another's locks (or, with a plain SELECT then
   * UPDATE, race and retry). With it, each worker just takes the next unlocked rows, so claims run
   * in parallel. The price: a claim may return fewer rows than available (some were locked by a
   * peer) and strict global ordering across workers is not guaranteed, only per-claim ordering.
   *
   * <p>One statement does three things: pick+lock, mark RUNNING with a lease and bump {@code
   * attempts}, and insert the {@code job_attempts} row, so a claimed job always has its attempt
   * record. {@code MATERIALIZED} stops the planner from inlining the CTE and re-evaluating the
   * locking SELECT.
   */
  @Transactional
  public List<Job> claim(String workerId, int limit, Duration lease, Collection<String> queues) {
    String queueFilter = queues.isEmpty() ? "" : " AND queue_name IN (:queues)";
    String sql =
        """
        WITH picked AS MATERIALIZED (
          SELECT id FROM jobs
          WHERE status = 'PENDING' AND run_at <= now()%s
          ORDER BY priority DESC, run_at
          LIMIT :limit
          FOR UPDATE SKIP LOCKED
        ),
        claimed AS (
          UPDATE jobs j
          SET status = 'RUNNING',
              locked_by = :worker,
              lease_expires_at = now() + make_interval(secs => :leaseSecs),
              attempts = j.attempts + 1,
              updated_at = now()
          FROM picked
          WHERE j.id = picked.id
          RETURNING j.*
        ),
        recorded AS (
          INSERT INTO job_attempts (job_id, attempt_number, worker_id)
          SELECT id, attempts, locked_by FROM claimed
        )
        SELECT %s FROM claimed
        """
            .formatted(queueFilter, JobRowMapper.COLUMNS);
    var params =
        new MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("worker", workerId)
            .addValue("leaseSecs", lease.toMillis() / 1000.0);
    if (!queues.isEmpty()) {
      params.addValue("queues", queues);
    }
    return jdbc.query(sql, params, jobMapper);
  }

  /**
   * Marks the attempt succeeded. <b>Fenced</b>: only applies while this worker still owns the job
   * for this attempt number. Returns false if ownership was lost (the result is discarded).
   */
  @Transactional
  public boolean complete(UUID jobId, String workerId, int attempt) {
    int updated =
        jdbc.update(
            """
            UPDATE jobs
            SET status = 'SUCCEEDED', locked_by = NULL, lease_expires_at = NULL,
                finished_at = now(), updated_at = now()
            WHERE id = :id AND status = 'RUNNING' AND locked_by = :worker AND attempts = :attempt
            """,
            fence(jobId, workerId, attempt));
    if (updated == 0) {
      return false;
    }
    finishAttempt(jobId, attempt, "SUCCEEDED", null, null);
    return true;
  }

  /**
   * Records a failed attempt (fenced like {@link #complete}). Retries go back to PENDING with
   * {@code run_at = now() + retryDelay} while attempts remain; otherwise the job becomes DEAD.
   *
   * <p>PLACEHOLDER policy for phase 3: the caller passes a fixed delay. Phase 4 replaces it with
   * exponential backoff + jitter and copies DEAD jobs to the dead-letter table.
   *
   * @return the new status, or empty if the worker no longer owned the job
   */
  @Transactional
  public Optional<JobStatus> fail(
      UUID jobId,
      String workerId,
      int attempt,
      boolean retryable,
      Duration retryDelay,
      String outcome,
      String errorMessage,
      String stackTrace) {
    var params =
        fence(jobId, workerId, attempt)
            .addValue("retryable", retryable)
            .addValue("delaySecs", retryDelay.toMillis() / 1000.0)
            .addValue("error", errorMessage);
    List<String> status =
        jdbc.query(
            """
            UPDATE jobs
            SET status = CASE WHEN :retryable AND attempts < max_attempts
                              THEN 'PENDING' ELSE 'DEAD' END,
                run_at = CASE WHEN :retryable AND attempts < max_attempts
                              THEN now() + make_interval(secs => :delaySecs) ELSE run_at END,
                finished_at = CASE WHEN :retryable AND attempts < max_attempts
                                   THEN NULL ELSE now() END,
                locked_by = NULL, lease_expires_at = NULL,
                last_error = :error, updated_at = now()
            WHERE id = :id AND status = 'RUNNING' AND locked_by = :worker AND attempts = :attempt
            RETURNING status
            """,
            params,
            (rs, i) -> rs.getString(1));
    if (status.isEmpty()) {
      return Optional.empty();
    }
    finishAttempt(jobId, attempt, outcome, errorMessage, stackTrace);
    return Optional.of(JobStatus.valueOf(status.get(0)));
  }

  private void finishAttempt(
      UUID jobId, int attempt, String outcome, String errorMessage, String stackTrace) {
    jdbc.update(
        """
        UPDATE job_attempts
        SET finished_at = now(), outcome = :outcome, error_message = :error, stack_trace = :trace
        WHERE job_id = :id AND attempt_number = :attempt
        """,
        new MapSqlParameterSource()
            .addValue("id", jobId)
            .addValue("attempt", attempt)
            .addValue("outcome", outcome)
            .addValue("error", errorMessage)
            .addValue("trace", stackTrace));
  }

  private static MapSqlParameterSource fence(UUID jobId, String workerId, int attempt) {
    return new MapSqlParameterSource()
        .addValue("id", jobId)
        .addValue("worker", workerId)
        .addValue("attempt", attempt);
  }
}
