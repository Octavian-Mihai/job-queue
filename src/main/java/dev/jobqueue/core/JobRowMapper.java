package dev.jobqueue.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

/** Maps a row of {@code jobs} (selected with {@link #COLUMNS}) to a {@link Job}. */
@Component
public class JobRowMapper implements RowMapper<Job> {

  public static final String COLUMNS =
      "id, queue_name, type, payload, status, priority, attempts, max_attempts, run_at, locked_by,"
          + " lease_expires_at, last_error, idempotency_key, created_at, updated_at, finished_at";

  private final ObjectMapper mapper;

  public JobRowMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public Job mapRow(ResultSet rs, int rowNum) throws SQLException {
    return new Job(
        rs.getObject("id", UUID.class),
        rs.getString("queue_name"),
        rs.getString("type"),
        readJson(rs.getString("payload")),
        JobStatus.valueOf(rs.getString("status")),
        rs.getInt("priority"),
        rs.getInt("attempts"),
        rs.getInt("max_attempts"),
        rs.getObject("run_at", OffsetDateTime.class),
        rs.getString("locked_by"),
        rs.getObject("lease_expires_at", OffsetDateTime.class),
        rs.getString("last_error"),
        rs.getString("idempotency_key"),
        rs.getObject("created_at", OffsetDateTime.class),
        rs.getObject("updated_at", OffsetDateTime.class),
        rs.getObject("finished_at", OffsetDateTime.class));
  }

  private com.fasterxml.jackson.databind.JsonNode readJson(String json) {
    try {
      return mapper.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("corrupt payload JSON in database", e);
    }
  }
}
