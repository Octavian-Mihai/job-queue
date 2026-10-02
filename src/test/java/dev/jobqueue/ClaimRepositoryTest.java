package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jobqueue.core.Job;
import dev.jobqueue.core.JobStatus;
import dev.jobqueue.worker.JobClaimRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/** The claim query in isolation (roles=api, so no worker loop is competing for rows). */
class ClaimRepositoryTest extends PostgresTestBase {

  private static final Duration LEASE = Duration.ofSeconds(30);

  @Autowired JobClaimRepository repo;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
  }

  private UUID insert(int priority, String runAtOffset, String queue) {
    return jdbc.queryForObject(
        "INSERT INTO jobs (type, priority, queue_name, run_at)"
            + " VALUES ('send-email', ?, ?, now() + ?::interval) RETURNING id",
        UUID.class,
        priority,
        queue,
        runAtOffset);
  }

  private UUID insert(int priority, String runAtOffset) {
    return insert(priority, runAtOffset, "default");
  }

  private static Set<UUID> ids(List<Job> jobs) {
    return jobs.stream().map(Job::id).collect(Collectors.toSet());
  }

  @Test
  void claimsHighestPriorityFirstThenEarliestRunAt() {
    UUID a = insert(0, "-10 minutes");
    UUID b = insert(5, "-1 minutes");
    UUID c = insert(5, "-5 minutes");
    UUID d = insert(0, "-1 minutes");

    assertThat(ids(repo.claim("w1", 2, LEASE, List.of()))).containsExactlyInAnyOrder(c, b);
    assertThat(ids(repo.claim("w1", 2, LEASE, List.of()))).containsExactlyInAnyOrder(a, d);
  }

  @Test
  void ignoresFutureAndNonPendingJobs() {
    insert(0, "1 hour");
    UUID cancelled = insert(0, "-1 minutes");
    jdbc.update("UPDATE jobs SET status='CANCELLED', finished_at=now() WHERE id=?", cancelled);
    UUID ready = insert(0, "-1 seconds");

    assertThat(ids(repo.claim("w1", 10, LEASE, List.of()))).containsExactly(ready);
    assertThat(repo.claim("w1", 10, LEASE, List.of())).isEmpty();
  }

  @Test
  void claimSetsOwnerLeaseAttemptAndRecordsAttemptRow() {
    UUID id = insert(0, "-1 seconds");

    Job job = repo.claim("worker-A", 1, LEASE, List.of()).get(0);

    assertThat(job.status()).isEqualTo(JobStatus.RUNNING);
    assertThat(job.lockedBy()).isEqualTo("worker-A");
    assertThat(job.attempts()).isEqualTo(1);
    Boolean leaseOk =
        jdbc.queryForObject(
            "SELECT lease_expires_at BETWEEN now() + interval '28 s' AND now() + interval '31 s'"
                + " FROM jobs WHERE id = ?",
            Boolean.class,
            id);
    assertThat(leaseOk).isTrue();
    var attempt = jdbc.queryForMap("SELECT * FROM job_attempts WHERE job_id = ?", id);
    assertThat(attempt.get("attempt_number")).isEqualTo(1);
    assertThat(attempt.get("worker_id")).isEqualTo("worker-A");
    assertThat(attempt.get("finished_at")).isNull();
  }

  @Test
  void honoursQueueFilter() {
    UUID inQ = insert(0, "-1 seconds", "emails");
    insert(0, "-1 seconds", "reports");

    assertThat(ids(repo.claim("w1", 10, LEASE, List.of("emails")))).containsExactly(inQ);
  }

  @Test
  void completionIsFencedToTheOwner() {
    UUID id = insert(0, "-1 seconds");
    Job job = repo.claim("owner", 1, LEASE, List.of()).get(0);

    assertThat(repo.complete(id, "someone-else", job.attempts())).isFalse();
    assertThat(repo.complete(id, "owner", job.attempts() + 1)).isFalse();
    assertThat(statusOf(id)).isEqualTo("RUNNING");

    assertThat(repo.complete(id, "owner", job.attempts())).isTrue();
    assertThat(statusOf(id)).isEqualTo("SUCCEEDED");
    assertThat(repo.complete(id, "owner", job.attempts())).isFalse();
  }

  @Test
  void concurrentClaimersNeverReceiveTheSameJob() throws Exception {
    int total = 2_000;
    jdbc.update(
        "INSERT INTO jobs (type, run_at) SELECT 'send-email', now() - interval '1 s'"
            + " FROM generate_series(1, ?)",
        total);
    int workers = 8;
    ExecutorService pool = Executors.newFixedThreadPool(workers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<List<UUID>>> futures = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      String workerId = "w" + w;
      futures.add(
          pool.submit(
              () -> {
                start.await();
                List<UUID> mine = new ArrayList<>();
                List<Job> batch;
                while (!(batch = repo.claim(workerId, 7, LEASE, List.of())).isEmpty()) {
                  batch.forEach(j -> mine.add(j.id()));
                }
                return mine;
              }));
    }
    start.countDown();
    List<UUID> all = new ArrayList<>();
    for (var f : futures) {
      all.addAll(f.get());
    }
    pool.shutdown();

    assertThat(all).hasSize(total);
    assertThat(new HashSet<>(all)).hasSize(total); // no duplicates => no double claims
    assertThat(Collections.frequency(all, all.get(0))).isEqualTo(1);
    assertThat(
            jdbc.queryForObject("SELECT count(*) FROM jobs WHERE status='RUNNING'", Integer.class))
        .isEqualTo(total);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class))
        .isEqualTo(total);
  }

  private String statusOf(UUID id) {
    return jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?", String.class, id);
  }
}
