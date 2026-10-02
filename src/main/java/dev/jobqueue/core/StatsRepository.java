package dev.jobqueue.core;

import java.util.EnumMap;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StatsRepository {

  private final JdbcTemplate jdbc;

  public StatsRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Three cheap aggregates. {@code GROUP BY status} scans the table, which is fine at this scale;
   * at hundreds of millions of rows you would maintain counters or use planner estimates instead.
   */
  public QueueStats snapshot() {
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (JobStatus s : JobStatus.values()) {
      counts.put(s, 0L);
    }
    jdbc.query(
        "SELECT status, count(*) FROM jobs GROUP BY status",
        rs -> {
          counts.put(JobStatus.valueOf(rs.getString(1)), rs.getLong(2));
        });
    Double oldest =
        jdbc.queryForObject(
            "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(run_at))), 0)::float8"
                + " FROM jobs WHERE status = 'PENDING' AND run_at <= now()",
            Double.class);
    Long dlq =
        jdbc.queryForObject(
            "SELECT count(*) FROM dead_letter_jobs WHERE replayed_at IS NULL", Long.class);
    return new QueueStats(counts, oldest == null ? 0 : oldest, dlq == null ? 0 : dlq);
  }
}
