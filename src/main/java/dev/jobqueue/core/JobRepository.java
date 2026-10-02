package dev.jobqueue.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** All SQL for the jobs table lives here so it stays explicit and reviewable. */
@Repository
public class JobRepository {

  private final NamedParameterJdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final JobRowMapper jobMapper;

  public JobRepository(
      NamedParameterJdbcTemplate jdbc, ObjectMapper mapper, JobRowMapper jobMapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.jobMapper = jobMapper;
  }

  /**
   * Inserts the job unless its idempotency key already exists.
   *
   * <p>{@code ON CONFLICT DO NOTHING} is what makes this race-safe: two concurrent inserts with the
   * same key serialize on the unique index; the loser blocks until the winner commits, then inserts
   * nothing. No check-then-insert window exists in application code.
   *
   * @return the new id, or empty if the key already existed
   */
  public Optional<UUID> insertIfAbsent(NewJob job) {
    String sql =
        """
        INSERT INTO jobs (queue_name, type, payload, priority, max_attempts, run_at, idempotency_key)
        VALUES (:queue, :type, :payload::jsonb, :priority, :maxAttempts,
                COALESCE(:runAt, now() + make_interval(secs => :delay)), :key)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id
        """;
    var params =
        new MapSqlParameterSource()
            .addValue("queue", job.queueName())
            .addValue("type", job.type())
            .addValue("payload", toJson(job.payload()))
            .addValue("priority", job.priority())
            .addValue("maxAttempts", job.maxAttempts())
            .addValue("runAt", job.runAt(), Types.TIMESTAMP_WITH_TIMEZONE)
            .addValue("delay", (double) job.delaySeconds())
            .addValue("key", job.idempotencyKey());
    return jdbc.query(sql, params, (rs, i) -> rs.getObject(1, UUID.class)).stream().findFirst();
  }

  public Optional<Job> findById(UUID id) {
    return jdbc
        .query(
            "SELECT " + JobRowMapper.COLUMNS + " FROM jobs WHERE id = :id",
            new MapSqlParameterSource("id", id),
            jobMapper)
        .stream()
        .findFirst();
  }

  public Optional<Job> findByIdempotencyKey(String key) {
    return jdbc
        .query(
            "SELECT " + JobRowMapper.COLUMNS + " FROM jobs WHERE idempotency_key = :key",
            new MapSqlParameterSource("key", key),
            jobMapper)
        .stream()
        .findFirst();
  }

  public List<JobAttempt> findAttempts(UUID jobId) {
    return jdbc.query(
        """
        SELECT attempt_number, worker_id, started_at, finished_at, outcome, error_message, stack_trace
        FROM job_attempts WHERE job_id = :id ORDER BY attempt_number
        """,
        new MapSqlParameterSource("id", jobId),
        (rs, i) ->
            new JobAttempt(
                rs.getInt("attempt_number"),
                rs.getString("worker_id"),
                rs.getObject("started_at", OffsetDateTime.class),
                rs.getObject("finished_at", OffsetDateTime.class),
                rs.getString("outcome"),
                rs.getString("error_message"),
                rs.getString("stack_trace")));
  }

  public PageResult<Job> list(JobStatus status, String type, String queue, int page, int size) {
    var where = new StringBuilder(" WHERE 1=1");
    var params = new MapSqlParameterSource();
    if (status != null) {
      where.append(" AND status = :status");
      params.addValue("status", status.name());
    }
    if (type != null) {
      where.append(" AND type = :type");
      params.addValue("type", type);
    }
    if (queue != null) {
      where.append(" AND queue_name = :queue");
      params.addValue("queue", queue);
    }
    Long total = jdbc.queryForObject("SELECT count(*) FROM jobs" + where, params, Long.class);
    params.addValue("limit", size).addValue("offset", (long) page * size);
    List<Job> items =
        new ArrayList<>(
            jdbc.query(
                "SELECT "
                    + JobRowMapper.COLUMNS
                    + " FROM jobs"
                    + where
                    + " ORDER BY created_at DESC, id LIMIT :limit OFFSET :offset",
                params,
                jobMapper));
    return new PageResult<>(items, page, size, total == null ? 0 : total);
  }

  /** Cancels only if still PENDING; returns false if the job was in any other state (or absent). */
  public boolean cancelIfPending(UUID id) {
    return jdbc.update(
            """
            UPDATE jobs SET status = 'CANCELLED', finished_at = now(), updated_at = now()
            WHERE id = :id AND status = 'PENDING'
            """,
            new MapSqlParameterSource("id", id))
        == 1;
  }

  private String toJson(JsonNode node) {
    try {
      return mapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("payload is not serializable", e);
    }
  }
}
