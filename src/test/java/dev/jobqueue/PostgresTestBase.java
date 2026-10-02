package dev.jobqueue;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Real PostgreSQL (no H2) shared by all integration tests.
 *
 * <p>Singleton container, started once per JVM and reaped by Testcontainers' Ryuk. Do not use
 * {@code @Container} here: it stops the container after each test class while Spring keeps the
 * cached context (and its connection pool) alive, so later classes would hit a dead database.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "jobqueue.roles=api") // no background worker unless a test opts in
public abstract class PostgresTestBase {

  @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  static {
    POSTGRES.start();
  }
}
