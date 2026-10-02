package dev.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jobqueue.PostgresTestBase;
import dev.jobqueue.core.Job;
import dev.jobqueue.core.JobStatus;
import dev.jobqueue.metrics.JobMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Regression test for a false "lost lease" found while screenshotting the dashboard: a fast job can
 * finish between the heartbeat's snapshot of in-flight jobs and its extend UPDATE. The job is then
 * no longer RUNNING, which looked exactly like a reclaimed lease, so the heartbeat counted a lost
 * lease and interrupted a handler that had simply finished.
 */
class HeartbeatRaceTest extends PostgresTestBase {

  @Autowired LeaseRepository leases;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;

  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final InFlightJobs inFlight = new InFlightJobs();
  private Heartbeater heartbeater;

  @BeforeEach
  void setUp() {
    jdbc.update("DELETE FROM jobs");
    heartbeater =
        new Heartbeater(
            leases,
            inFlight,
            new JobMetrics(registry),
            "w1",
            Duration.ofSeconds(30),
            Duration.ofSeconds(10));
  }

  /** A claim registered as in-flight whose database row is already SUCCEEDED (it just finished). */
  private InFlightJobs.Handle registerFinishedInDb(Thread thread) {
    UUID id =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, status, attempts, finished_at) VALUES ('send-email',"
                + " 'SUCCEEDED', 1, now()) RETURNING id",
            UUID.class);
    Job job =
        new Job(
            id,
            "default",
            "send-email",
            json.createObjectNode(),
            JobStatus.RUNNING,
            0,
            1,
            3,
            OffsetDateTime.now(),
            "w1",
            OffsetDateTime.now().plusSeconds(30),
            null,
            null,
            OffsetDateTime.now(),
            OffsetDateTime.now(),
            null);
    return inFlight.register(job, thread);
  }

  private Thread parkedThread(AtomicBoolean interrupted, CountDownLatch started) {
    Thread t =
        new Thread(
            () -> {
              started.countDown();
              try {
                Thread.sleep(30_000);
              } catch (InterruptedException e) {
                interrupted.set(true);
              }
            });
    t.setDaemon(true);
    t.start();
    return t;
  }

  private double lostCount() {
    var c = registry.find("jobqueue.heartbeat.lost").counter();
    return c == null ? 0 : c.count();
  }

  @Test
  void aJobThatFinishesBetweenTheSnapshotAndTheExtendIsNotReportedLostOrInterrupted()
      throws Exception {
    var interrupted = new AtomicBoolean();
    var started = new CountDownLatch(1);
    Thread handler = parkedThread(interrupted, started);
    started.await();
    InFlightJobs.Handle handle = registerFinishedInDb(handler);
    // Reproduce the race exactly: the heartbeat has already taken its snapshot (the handle is not
    // yet finishing), and the job completes while the extend UPDATE is in flight. The executor
    // marks the handle finishing BEFORE it writes the outcome, so it is set by the time the
    // UPDATE reports the row as no longer RUNNING.
    var racing =
        new LeaseRepository(jdbc) {
          @Override
          public java.util.Set<UUID> extend(
              String workerId, java.util.List<Lease> claims, Duration lease) {
            handle.markFinishing();
            return super.extend(workerId, claims, lease);
          }
        };
    var racingHeartbeater =
        new Heartbeater(
            racing,
            inFlight,
            new JobMetrics(registry),
            "w1",
            Duration.ofSeconds(30),
            Duration.ofSeconds(10));

    racingHeartbeater.beat();
    handler.join(300);

    assertThat(interrupted.get()).isFalse();
    assertThat(handle.leaseLost()).isFalse();
    assertThat(lostCount()).isZero();
    handler.interrupt();
  }

  @Test
  void aHandleThatIsAlreadyFinishingIsNotEvenHeartbeated() throws Exception {
    var started = new CountDownLatch(1);
    Thread handler = parkedThread(new AtomicBoolean(), started);
    started.await();
    InFlightJobs.Handle handle = registerFinishedInDb(handler);
    handle.markFinishing();

    heartbeater.beat();

    assertThat(lostCount()).isZero();
    assertThat(handle.leaseLost()).isFalse();
    handler.interrupt();
  }

  @Test
  void aGenuinelyLostLeaseIsStillDetectedAndInterrupted() throws Exception {
    var interrupted = new AtomicBoolean();
    var started = new CountDownLatch(1);
    Thread handler = parkedThread(interrupted, started);
    started.await();
    InFlightJobs.Handle handle =
        registerFinishedInDb(handler); // row gone from RUNNING, not finishing

    heartbeater.beat();
    handler.join(2_000);

    assertThat(interrupted.get()).isTrue();
    assertThat(handle.leaseLost()).isTrue();
    assertThat(lostCount()).isEqualTo(1);
  }
}
