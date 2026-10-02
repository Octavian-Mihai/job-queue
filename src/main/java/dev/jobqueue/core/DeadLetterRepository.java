package dev.jobqueue.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class DeadLetterRepository {

  private static final String COLUMNS =
      "id, job_id, queue_name, type, payload, priority, attempts, max_attempts, last_error,"
          + " reason, job_created_at, dead_at, replayed_at, replayed_job_id";

  private final NamedParameterJdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final RowMapper<DeadLetter> rowMapper;

  public DeadLetterRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.rowMapper =
        (rs, i) -> {
          try {
            return new DeadLetter(
                rs.getObject("id", UUID.class),
                rs.getObject("job_id", UUID.class),
                rs.getString("queue_name"),
                rs.getString("type"),
                mapper.readTree(rs.getString("payload")),
                rs.getInt("priority"),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                rs.getString("last_error"),
                rs.getString("reason"),
                rs.getObject("job_created_at", OffsetDateTime.class),
                rs.getObject("dead_at", OffsetDateTime.class),
                rs.getObject("replayed_at", OffsetDateTime.class),
                rs.getObject("replayed_job_id", UUID.class));
          } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt payload JSON in dead_letter_jobs", e);
          }
        };
  }

  public Optional<DeadLetter> findById(UUID id) {
    return jdbc
        .query(
            "SELECT " + COLUMNS + " FROM dead_letter_jobs WHERE id = :id",
            new MapSqlParameterSource("id", id),
            rowMapper)
        .stream()
        .findFirst();
  }

  public PageResult<DeadLetter> list(DeadLetterFilter filter, int page, int size) {
    var params = new MapSqlParameterSource();
    List<String> conditions = conditions(filter, params);
    String where = conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    Long total =
        jdbc.queryForObject("SELECT count(*) FROM dead_letter_jobs" + where, params, Long.class);
    params.addValue("limit", size).addValue("offset", (long) page * size);
    List<DeadLetter> items =
        jdbc.query(
            "SELECT "
                + COLUMNS
                + " FROM dead_letter_jobs"
                + where
                + " ORDER BY dead_at DESC, id LIMIT :limit OFFSET :offset",
            params,
            rowMapper);
    return new PageResult<>(items, page, size, total == null ? 0 : total);
  }

  /**
   * Replays dead-lettered jobs by creating <b>new</b> jobs (fresh id, {@code attempts = 0}, run
   * now) and marking each DLQ entry replayed, all in one atomic statement.
   *
   * <p>{@code replayed_at IS NULL} plus {@code FOR UPDATE SKIP LOCKED} makes double replay
   * impossible: two concurrent replays cannot both pick the same entry, so a job is never
   * resurrected twice. The original (DEAD) job row is left untouched as history.
   *
   * @param onlyId restrict to one DLQ entry, or null for "all matching the filter"
   * @return ids of the newly created jobs
   */
  @Transactional
  public List<UUID> replay(UUID onlyId, DeadLetterFilter filter, int limit) {
    var params = new MapSqlParameterSource("limit", limit);
    List<String> conditions = new ArrayList<>(conditions(filter, params));
    conditions.add("replayed_at IS NULL"); // never replay the same entry twice
    if (onlyId != null) {
      conditions.add("id = :onlyId");
      params.addValue("onlyId", onlyId);
    }
    String sql =
        """
        WITH to_replay AS (
          SELECT id, gen_random_uuid() AS new_id
          FROM dead_letter_jobs
          %s
          ORDER BY dead_at
          LIMIT :limit
          FOR UPDATE SKIP LOCKED
        ),
        marked AS (
          UPDATE dead_letter_jobs d
          SET replayed_at = now(), replayed_job_id = r.new_id
          FROM to_replay r
          WHERE d.id = r.id
          RETURNING d.queue_name, d.type, d.payload, d.priority, d.max_attempts, d.replayed_job_id
        ),
        inserted AS (
          INSERT INTO jobs (id, queue_name, type, payload, priority, max_attempts)
          SELECT replayed_job_id, queue_name, type, payload, priority, max_attempts FROM marked
          RETURNING id
        )
        SELECT id FROM inserted
        """
            .formatted("WHERE " + String.join(" AND ", conditions));
    return jdbc.query(sql, params, (rs, i) -> rs.getObject(1, UUID.class));
  }

  private static List<String> conditions(DeadLetterFilter f, MapSqlParameterSource params) {
    List<String> out = new ArrayList<>();
    if (f == null) {
      return out;
    }
    if (f.type() != null) {
      out.add("type = :type");
      params.addValue("type", f.type());
    }
    if (f.queue() != null) {
      out.add("queue_name = :queue");
      params.addValue("queue", f.queue());
    }
    if (f.reason() != null) {
      out.add("reason = :reason");
      params.addValue("reason", f.reason());
    }
    if (f.replayed() != null) {
      out.add(f.replayed() ? "replayed_at IS NOT NULL" : "replayed_at IS NULL");
    }
    return out;
  }
}
