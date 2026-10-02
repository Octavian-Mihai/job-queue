package dev.jobqueue.worker;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Lease maintenance SQL: extend (heartbeat), reclaim expired leases (reaper) and release (graceful
 * shutdown). Every statement is conditional on {@code status='RUNNING'} and, where it acts on a
 * specific claim, on {@code locked_by} and the attempt number: that is the fence.
 *
 * <p>All time comparisons use the database clock ({@code now()}), never a worker's clock, so clock
 * skew between machines cannot cause premature or missed expiry.
 */
@Repository
public class LeaseRepository {

  /** A job returned to the queue (or buried) by the reaper. */
  public record Reaped(UUID jobId, String newStatus) {}

  private final JdbcTemplate jdbc;

  public LeaseRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Heartbeat: pushes the lease of every listed claim out to {@code now() + lease}, in one
   * statement for the whole worker. Only claims this worker still owns for that attempt are
   * extended.
   *
   * @return the job ids that were extended; any listed claim missing from the result has been lost
   *     (reclaimed by the reaper, cancelled, or finished by someone else)
   */
  public Set<UUID> extend(String workerId, List<Lease> leases, Duration lease) {
    if (leases.isEmpty()) {
      return Set.of();
    }
    List<UUID> ids =
        jdbc.query(
            con -> {
              PreparedStatement ps =
                  con.prepareStatement(
                      """
                      UPDATE jobs j
                      SET lease_expires_at = now() + make_interval(secs => ?), updated_at = now()
                      FROM unnest(?::uuid[], ?::int[]) AS t(id, attempt)
                      WHERE j.id = t.id AND j.attempts = t.attempt
                        AND j.locked_by = ? AND j.status = 'RUNNING'
                      RETURNING j.id
                      """);
              ps.setDouble(1, lease.toMillis() / 1000.0);
              bindLeases(con, ps, 2, leases);
              ps.setString(4, workerId);
              return ps;
            },
            (rs, i) -> rs.getObject(1, UUID.class));
    return new HashSet<>(ids);
  }

  /**
   * The reaper: returns RUNNING jobs whose lease has expired to the queue, or buries them if their
   * attempts are used up. Safe to run on every worker at once.
   *
   * <p>{@code FOR UPDATE SKIP LOCKED} lets concurrent reapers split the work without double
   * reclaiming, and it also resolves the race with a late heartbeat: if the heartbeat's UPDATE got
   * the row first, the reaper's re-check sees the fresh lease and skips it; if the reaper got it
   * first, the heartbeat then finds the row no longer RUNNING and reports the lease lost.
   *
   * <p>A crash <b>consumes the attempt</b> (it stays counted): if a job keeps killing its worker
   * (OOM, poison payload) it must eventually be dead-lettered rather than loop forever. One
   * statement flips the job, closes its attempt row as {@code LEASE_EXPIRED}, and writes the DLQ
   * entry for jobs that die.
   */
  @Transactional
  public List<Reaped> reapExpired(int limit) {
    return jdbc.query(
        """
        WITH expired AS (
          SELECT id FROM jobs
          WHERE status = 'RUNNING' AND lease_expires_at < now()
          ORDER BY lease_expires_at
          LIMIT ?
          FOR UPDATE SKIP LOCKED
        ),
        reaped AS (
          UPDATE jobs j
          SET status = CASE WHEN j.attempts < j.max_attempts THEN 'PENDING' ELSE 'DEAD' END,
              run_at = CASE WHEN j.attempts < j.max_attempts THEN now() ELSE j.run_at END,
              finished_at = CASE WHEN j.attempts < j.max_attempts THEN NULL ELSE now() END,
              locked_by = NULL,
              lease_expires_at = NULL,
              last_error = 'lease expired: worker stopped heartbeating (presumed crashed)',
              updated_at = now()
          FROM expired
          WHERE j.id = expired.id
          RETURNING j.*
        ),
        closed AS (
          UPDATE job_attempts a
          SET finished_at = now(), outcome = 'LEASE_EXPIRED',
              error_message = 'lease expired: worker ' || a.worker_id || ' stopped heartbeating'
          FROM reaped r
          WHERE a.job_id = r.id AND a.attempt_number = r.attempts AND a.outcome IS NULL
        ),
        buried AS (
          INSERT INTO dead_letter_jobs
            (job_id, queue_name, type, payload, priority, attempts, max_attempts, last_error,
             reason, job_created_at)
          SELECT id, queue_name, type, payload, priority, attempts, max_attempts, last_error,
                 '%s', created_at
          FROM reaped WHERE status = 'DEAD'
        )
        SELECT id, status FROM reaped
        """
            .formatted(DeadReason.MAX_ATTEMPTS_EXCEEDED),
        ps -> ps.setInt(1, limit),
        (rs, i) -> new Reaped(rs.getObject("id", UUID.class), rs.getString("status")));
  }

  /**
   * Graceful shutdown: hands unfinished claims back to the queue immediately instead of making them
   * wait out their lease. Unlike a crash this does <b>not</b> consume an attempt: {@code attempts}
   * is decremented and the half-finished attempt row removed, so rolling deploys can never
   * dead-letter a job. Fenced like completion, so a job that finished in the meantime is untouched.
   *
   * @return how many claims were released
   */
  @Transactional
  public int release(String workerId, List<Lease> leases) {
    if (leases.isEmpty()) {
      return 0;
    }
    List<UUID> released =
        jdbc.query(
            con -> {
              PreparedStatement ps =
                  con.prepareStatement(
                      """
                      WITH released AS (
                        UPDATE jobs j
                        SET status = 'PENDING', run_at = now(), locked_by = NULL,
                            lease_expires_at = NULL, attempts = j.attempts - 1, updated_at = now()
                        FROM unnest(?::uuid[], ?::int[]) AS t(id, attempt)
                        WHERE j.id = t.id AND j.attempts = t.attempt
                          AND j.locked_by = ? AND j.status = 'RUNNING'
                        RETURNING j.id, j.attempts + 1 AS released_attempt
                      ),
                      removed AS (
                        DELETE FROM job_attempts a USING released r
                        WHERE a.job_id = r.id AND a.attempt_number = r.released_attempt
                      )
                      SELECT id FROM released
                      """);
              bindLeases(con, ps, 1, leases);
              ps.setString(3, workerId);
              return ps;
            },
            (rs, i) -> rs.getObject(1, UUID.class));
    return released.size();
  }

  private static void bindLeases(
      java.sql.Connection con, PreparedStatement ps, int firstIndex, List<Lease> leases)
      throws SQLException {
    Array ids = con.createArrayOf("uuid", leases.stream().map(Lease::jobId).toArray());
    Array attempts = con.createArrayOf("int4", leases.stream().map(Lease::attempt).toArray());
    ps.setArray(firstIndex, ids);
    ps.setArray(firstIndex + 1, attempts);
  }
}
