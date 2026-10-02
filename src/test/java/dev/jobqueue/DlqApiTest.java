package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

class DlqApiTest extends PostgresTestBase {

  @Autowired TestRestTemplate http;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
    jdbc.update("DELETE FROM dead_letter_jobs");
  }

  /** A DEAD job with a 2-attempt history and its DLQ entry; returns the DLQ entry id. */
  private UUID insertDead(String type, String queue, String reason) {
    UUID jobId =
        jdbc.queryForObject(
            "INSERT INTO jobs (type, queue_name, status, attempts, max_attempts, priority, payload,"
                + " last_error, finished_at) VALUES (?, ?, 'DEAD', 2, 2, 4, '{\"n\":42}', 'kaboom', now())"
                + " RETURNING id",
            UUID.class,
            type,
            queue);
    jdbc.update(
        "INSERT INTO job_attempts (job_id, attempt_number, worker_id, outcome, error_message,"
            + " finished_at) VALUES (?, 1, 'w1', 'FAILED_RETRYABLE', 'first', now()),"
            + " (?, 2, 'w2', 'FAILED_RETRYABLE', 'second', now())",
        jobId,
        jobId);
    return jdbc.queryForObject(
        "INSERT INTO dead_letter_jobs (job_id, queue_name, type, payload, priority, attempts,"
            + " max_attempts, last_error, reason, job_created_at)"
            + " SELECT id, queue_name, type, payload, priority, attempts, max_attempts, last_error,"
            + " ?, created_at FROM jobs WHERE id = ? RETURNING id",
        UUID.class,
        reason,
        jobId);
  }

  private ResponseEntity<JsonNode> post(String path) {
    return http.exchange(path, HttpMethod.POST, HttpEntity.EMPTY, JsonNode.class);
  }

  private JsonNode get(String path) {
    return http.getForEntity(path, JsonNode.class).getBody();
  }

  @Test
  void listsFiltersAndPaginates() {
    insertDead("send-email", "q1", "NON_RETRYABLE");
    insertDead("send-email", "q1", "MAX_ATTEMPTS_EXCEEDED");
    insertDead("deliver-webhook", "q2", "MAX_ATTEMPTS_EXCEEDED");

    assertThat(get("/dlq").get("total").asInt()).isEqualTo(3);
    assertThat(get("/dlq?size=2").get("items")).hasSize(2);
    assertThat(get("/dlq?size=2&page=1").get("items")).hasSize(1);
    assertThat(get("/dlq?type=send-email").get("total").asInt()).isEqualTo(2);
    assertThat(get("/dlq?queue=q2").get("total").asInt()).isEqualTo(1);
    assertThat(get("/dlq?reason=NON_RETRYABLE").get("total").asInt()).isEqualTo(1);
    assertThat(get("/dlq?replayed=true").get("total").asInt()).isZero();
    assertThat(get("/dlq?replayed=false").get("total").asInt()).isEqualTo(3);
  }

  @Test
  void inspectShowsEntryAndFullAttemptHistory() {
    UUID id = insertDead("send-email", "default", "MAX_ATTEMPTS_EXCEEDED");

    JsonNode d = get("/dlq/" + id);

    assertThat(d.get("type").asText()).isEqualTo("send-email");
    assertThat(d.get("reason").asText()).isEqualTo("MAX_ATTEMPTS_EXCEEDED");
    assertThat(d.get("lastError").asText()).isEqualTo("kaboom");
    assertThat(d.get("payload").get("n").asInt()).isEqualTo(42);
    assertThat(d.get("attempts")).hasSize(2);
    assertThat(d.get("attempts").get(0).get("errorMessage").asText()).isEqualTo("first");
    assertThat(d.get("attempts").get(1).get("workerId").asText()).isEqualTo("w2");
  }

  @Test
  void inspectUnknownIs404() {
    var res = http.getForEntity("/dlq/" + UUID.randomUUID(), JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void replayOneCreatesAFreshJobAndRecordsReplay() {
    UUID id = insertDead("send-email", "q1", "NON_RETRYABLE");

    var res = post("/dlq/" + id + "/replay");

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    UUID newJobId = UUID.fromString(res.getBody().get("jobIds").get(0).asText());
    var job = jdbc.queryForMap("SELECT * FROM jobs WHERE id = ?", newJobId);
    assertThat(job.get("status")).isEqualTo("PENDING");
    assertThat(job.get("attempts")).isEqualTo(0);
    assertThat(job.get("queue_name")).isEqualTo("q1");
    assertThat(job.get("type")).isEqualTo("send-email");
    assertThat(job.get("priority")).isEqualTo(4);
    assertThat(job.get("max_attempts")).isEqualTo(2);
    assertThat(job.get("payload").toString()).contains("42");
    var dlq = jdbc.queryForMap("SELECT * FROM dead_letter_jobs WHERE id = ?", id);
    assertThat(dlq.get("replayed_at")).isNotNull();
    assertThat(dlq.get("replayed_job_id")).isEqualTo(newJobId);
    assertThat(
            jdbc.queryForObject("SELECT count(*) FROM jobs WHERE status = 'DEAD'", Integer.class))
        .isEqualTo(1); // original kept as history
  }

  @Test
  void replayingTwiceIsAConflict() {
    UUID id = insertDead("send-email", "default", "NON_RETRYABLE");
    assertThat(post("/dlq/" + id + "/replay").getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(post("/dlq/" + id + "/replay").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE status = 'PENDING'", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void replayUnknownIs404() {
    assertThat(post("/dlq/" + UUID.randomUUID() + "/replay").getStatusCode())
        .isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void replayAllHonoursFiltersAndSkipsAlreadyReplayed() {
    UUID a = insertDead("send-email", "q1", "NON_RETRYABLE");
    insertDead("send-email", "q1", "MAX_ATTEMPTS_EXCEEDED");
    insertDead("deliver-webhook", "q1", "MAX_ATTEMPTS_EXCEEDED");
    post("/dlq/" + a + "/replay"); // already replayed: must not be replayed again

    var res = post("/dlq/replay?type=send-email");

    assertThat(res.getBody().get("replayed").asInt()).isEqualTo(1);
    assertThat(get("/dlq?replayed=false").get("total").asInt()).isEqualTo(1); // the webhook one
    assertThat(post("/dlq/replay?type=send-email").getBody().get("replayed").asInt()).isZero();
    assertThat(post("/dlq/replay").getBody().get("replayed").asInt()).isEqualTo(1);
    assertThat(get("/dlq?replayed=false").get("total").asInt()).isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE status = 'PENDING'", Integer.class))
        .isEqualTo(3);
  }

  @Test
  void replayAllRespectsTheLimitOldestFirst() {
    for (int i = 0; i < 5; i++) {
      insertDead("send-email", "default", "NON_RETRYABLE");
    }
    assertThat(post("/dlq/replay?limit=2").getBody().get("replayed").asInt()).isEqualTo(2);
    assertThat(get("/dlq?replayed=false").get("total").asInt()).isEqualTo(3);
  }

  @Test
  void concurrentReplaysOfOneEntryCreateExactlyOneJob() throws Exception {
    UUID id = insertDead("send-email", "default", "NON_RETRYABLE");
    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<HttpStatus>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      futures.add(
          pool.submit(
              () -> {
                start.await();
                return HttpStatus.valueOf(post("/dlq/" + id + "/replay").getStatusCode().value());
              }));
    }
    start.countDown();
    Set<HttpStatus> seen = new HashSet<>();
    int created = 0;
    for (var f : futures) {
      HttpStatus s = f.get();
      seen.add(s);
      if (s == HttpStatus.CREATED) {
        created++;
      }
    }
    pool.shutdown();

    assertThat(created).isEqualTo(1);
    assertThat(seen).isSubsetOf(HttpStatus.CREATED, HttpStatus.CONFLICT);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE status = 'PENDING'", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void replayAllValidatesLimit() {
    assertThat(post("/dlq/replay?limit=0").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(post("/dlq/replay?limit=99999").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }
}
