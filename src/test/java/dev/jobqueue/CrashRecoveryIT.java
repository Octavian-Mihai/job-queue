package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The real thing: two worker <b>processes</b> (the packaged jar, {@code java -jar}) share a real
 * PostgreSQL; one is killed with SIGKILL while holding running jobs. No cooperation from the dying
 * process is possible, so the only thing that can save the jobs is lease expiry plus the surviving
 * worker's reaper.
 *
 * <p>Runs under failsafe ({@code *IT}) after {@code package}, because it needs {@code
 * target/app.jar}.
 */
class CrashRecoveryIT {

  private static final int JOBS = 24;

  private PostgreSQLContainer<?> postgres;
  private final List<Process> processes = new ArrayList<>();

  @BeforeEach
  void startDatabase() {
    postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    postgres.start();
  }

  @AfterEach
  void cleanUp() {
    processes.forEach(Process::destroyForcibly);
    postgres.stop();
  }

  @Test
  void killingAWorkerProcessMidJobLosesNothingAndAnotherWorkerFinishesTheJobs() throws Exception {
    Path jar = Path.of("target", "app.jar").toAbsolutePath();
    assertThat(jar).as("run via `mvn verify` so the jar is built first").exists();

    Process workerA = startWorker(jar, "worker-A");
    awaitSchema(); // worker-A ran the Flyway migrations
    Process workerB = startWorker(jar, "worker-B");

    // 24 eight-second jobs; each worker runs 8 at a time, so both will hold running jobs.
    try (Connection c = connect();
        Statement s = c.createStatement()) {
      s.execute(
          "INSERT INTO jobs (type, payload, max_attempts) SELECT 'generate-report',"
              + " '{\"sleepMs\":8000}'::jsonb, 5 FROM generate_series(1, "
              + JOBS
              + ")");
    }

    await()
        .atMost(Duration.ofSeconds(60))
        .until(
            () ->
                scalar("SELECT count(*) FROM jobs WHERE status='RUNNING' AND locked_by='worker-A'")
                        >= 1
                    && scalar(
                            "SELECT count(*) FROM jobs WHERE status='RUNNING' AND locked_by='worker-B'")
                        >= 1);
    long heldByVictim =
        scalar("SELECT count(*) FROM jobs WHERE status='RUNNING' AND locked_by='worker-A'");
    assertThat(heldByVictim).isGreaterThanOrEqualTo(1);

    workerA.destroyForcibly(); // SIGKILL: no shutdown hook, no lease release, nothing
    workerA.waitFor();
    assertThat(workerA.isAlive()).isFalse();

    await()
        .atMost(Duration.ofSeconds(120))
        .until(() -> scalar("SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'") == JOBS);

    // nothing lost, nothing dead, nothing stuck
    assertThat(scalar("SELECT count(*) FROM jobs")).isEqualTo(JOBS);
    assertThat(scalar("SELECT count(*) FROM jobs WHERE status <> 'SUCCEEDED'")).isZero();
    assertThat(scalar("SELECT count(*) FROM dead_letter_jobs")).isZero();
    // every job the victim was holding was reclaimed after its lease expired...
    long reclaimed = scalar("SELECT count(*) FROM job_attempts WHERE outcome = 'LEASE_EXPIRED'");
    assertThat(reclaimed).isGreaterThanOrEqualTo(heldByVictim);
    // ...and finished by the survivor, as a later attempt
    assertThat(
            scalar(
                "SELECT count(*) FROM job_attempts a WHERE a.outcome = 'LEASE_EXPIRED' AND"
                    + " a.worker_id = 'worker-A' AND EXISTS (SELECT 1 FROM job_attempts b WHERE"
                    + " b.job_id = a.job_id AND b.outcome = 'SUCCEEDED' AND b.worker_id = 'worker-B')"))
        .isEqualTo(reclaimed);
    assertThat(scalar("SELECT count(*) FROM job_attempts WHERE outcome IS NULL")).isZero();
    assertThat(workerB.isAlive()).isTrue(); // the survivor never went down
  }

  private Process startWorker(Path jar, String workerId) throws IOException {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    ProcessBuilder pb = new ProcessBuilder(java, "-Xmx256m", "-jar", jar.toString());
    var env = pb.environment();
    env.put("DB_URL", postgres.getJdbcUrl());
    env.put("DB_USER", postgres.getUsername());
    env.put("DB_PASSWORD", postgres.getPassword());
    env.put("JOBQUEUE_ROLES", "worker");
    env.put("JOBQUEUE_WORKER_ID", workerId);
    env.put("SERVER_PORT", "0");
    env.put("JOBQUEUE_WORKER_CONCURRENCY", "8");
    env.put("JOBQUEUE_WORKER_LEASE_DURATION", "4s");
    env.put("JOBQUEUE_WORKER_HEARTBEAT_INTERVAL", "1s");
    env.put("JOBQUEUE_WORKER_REAPER_INTERVAL", "1s");
    env.put("JOBQUEUE_WORKER_POLL_INTERVAL", "100ms");
    Path logs = Path.of("target", "crash-recovery");
    Files.createDirectories(logs);
    File log = logs.resolve(workerId + ".log").toFile();
    pb.redirectErrorStream(true).redirectOutput(log);
    Process p = pb.start();
    processes.add(p);
    return p;
  }

  private void awaitSchema() {
    await()
        .atMost(Duration.ofSeconds(90))
        .ignoreExceptions()
        .until(
            () ->
                scalar("SELECT count(*) FROM information_schema.tables WHERE table_name = 'jobs'")
                    == 1);
  }

  private Connection connect() throws SQLException {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private long scalar(String sql) throws SQLException {
    try (Connection c = connect();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
