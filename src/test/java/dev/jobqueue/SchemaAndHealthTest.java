package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

class SchemaAndHealthTest extends PostgresTestBase {

  @Autowired TestRestTemplate http;
  @Autowired JdbcTemplate jdbc;

  @Test
  void healthEndpointReportsUpWithDatabase() {
    var response = http.getForEntity("/actuator/health", String.class);
    assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    assertThat(response.getBody()).contains("\"status\":\"UP\"").contains("db");
  }

  @Test
  void flywayCreatedAllTables() {
    var tables =
        jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            String.class);
    assertThat(tables).contains("jobs", "job_attempts", "dead_letter_jobs");
  }

  @Test
  void invalidStatusIsRejected() {
    assertThatThrownBy(() -> jdbc.update("INSERT INTO jobs (type, status) VALUES ('t', 'BOGUS')"))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void runningJobMustHaveOwnerAndLease() {
    assertThatThrownBy(() -> jdbc.update("INSERT INTO jobs (type, status) VALUES ('t', 'RUNNING')"))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void idempotencyKeyIsUniqueButNullsAreAllowedRepeatedly() {
    jdbc.update("INSERT INTO jobs (type) VALUES ('t'), ('t')");
    jdbc.update("INSERT INTO jobs (type, idempotency_key) VALUES ('t', 'k1')");
    assertThatThrownBy(
            () -> jdbc.update("INSERT INTO jobs (type, idempotency_key) VALUES ('t', 'k1')"))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void claimIndexIsPartialOnPending() {
    String def =
        jdbc.queryForObject(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_jobs_claim'", String.class);
    assertThat(def).contains("WHERE (status = 'PENDING'");
  }
}
