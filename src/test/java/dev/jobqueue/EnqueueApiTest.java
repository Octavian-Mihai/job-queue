package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

class EnqueueApiTest extends PostgresTestBase {

  @Autowired TestRestTemplate http;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM jobs");
  }

  private ResponseEntity<JsonNode> post(String path, Object body, String idempotencyKey) {
    var headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    if (idempotencyKey != null) {
      headers.set("Idempotency-Key", idempotencyKey);
    }
    return http.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
  }

  private ResponseEntity<JsonNode> enqueue(Map<String, Object> body) {
    return post("/jobs", body, null);
  }

  @Test
  void createsPendingJobWithDefaults() {
    var res = enqueue(Map.of("type", "send-email", "payload", Map.of("to", "a@b.c")));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    JsonNode job = res.getBody();
    assertThat(res.getHeaders().getLocation().getPath())
        .isEqualTo("/jobs/" + job.get("id").asText());
    assertThat(job.get("status").asText()).isEqualTo("PENDING");
    assertThat(job.get("queueName").asText()).isEqualTo("default");
    assertThat(job.get("priority").asInt()).isZero();
    assertThat(job.get("attempts").asInt()).isZero();
    assertThat(job.get("maxAttempts").asInt()).isEqualTo(5);
    assertThat(job.get("payload").get("to").asText()).isEqualTo("a@b.c");
    assertThat(job.get("attempts")).isNotNull();
    assertThat(job.get("lockedBy").isNull()).isTrue();
  }

  @Test
  void missingPayloadBecomesEmptyObject() {
    var job = enqueue(Map.of("type", "t")).getBody();
    assertThat(job.get("payload").isObject()).isTrue();
    assertThat(job.get("payload")).isEmpty();
  }

  @Test
  void delaySecondsSchedulesInTheFutureUsingDbClock() {
    var job = enqueue(Map.of("type", "t", "delaySeconds", 3600)).getBody();
    Boolean future =
        jdbc.queryForObject(
            "SELECT run_at > now() + interval '59 minutes' AND run_at < now() + interval '61 minutes'"
                + " FROM jobs WHERE id = ?::uuid",
            Boolean.class,
            job.get("id").asText());
    assertThat(future).isTrue();
  }

  @Test
  void explicitRunAtIsHonoured() {
    var job = enqueue(Map.of("type", "t", "runAt", "2035-01-01T00:00:00Z")).getBody();
    assertThat(job.get("runAt").asText()).startsWith("2035-01-01T00:00:00");
  }

  @Test
  void invalidRequestReturnsProblemDetailWithFieldErrors() {
    var res = enqueue(Map.of("type", "", "maxAttempts", 0, "priority", 5000));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(res.getHeaders().getContentType().toString()).startsWith("application/problem+json");
    JsonNode errors = res.getBody().get("errors");
    assertThat(errors.has("type")).isTrue();
    assertThat(errors.has("maxAttempts")).isTrue();
    assertThat(errors.has("priority")).isTrue();
    assertThat(res.getBody().get("status").asInt()).isEqualTo(400);
  }

  @Test
  void delayAndRunAtAreMutuallyExclusive() {
    var res = enqueue(Map.of("type", "t", "delaySeconds", 5, "runAt", "2035-01-01T00:00:00Z"));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(res.getBody().get("errors").toString()).contains("mutually exclusive");
  }

  @Test
  void malformedJsonIsProblemDetail() {
    var headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    var res =
        http.exchange(
            "/jobs", HttpMethod.POST, new HttpEntity<>("{not json", headers), JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(res.getHeaders().getContentType().toString()).startsWith("application/problem+json");
  }

  @Test
  void tooLongIdempotencyKeyIsRejected() {
    var res = post("/jobs", Map.of("type", "t"), "k".repeat(256));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void repeatedIdempotencyKeyReturnsSameJobWith200() {
    var first = post("/jobs", Map.of("type", "t"), "key-1");
    var second = post("/jobs", Map.of("type", "t"), "key-1");

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
    assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(1);
  }

  @Test
  void differentKeysCreateDifferentJobs() {
    var a = post("/jobs", Map.of("type", "t"), "a");
    var b = post("/jobs", Map.of("type", "t"), "b");
    assertThat(a.getBody().get("id")).isNotEqualTo(b.getBody().get("id"));
  }

  @Test
  void concurrentSubmissionsWithSameKeyCreateExactlyOneJob() throws Exception {
    int threads = 32;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<ResponseEntity<JsonNode>>> futures = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      futures.add(
          pool.submit(
              () -> {
                start.await();
                return post("/jobs", Map.of("type", "t"), "race-key");
              }));
    }
    start.countDown();

    Set<String> ids = new HashSet<>();
    int created = 0;
    for (var f : futures) {
      var res = f.get();
      assertThat(res.getStatusCode().is2xxSuccessful()).isTrue();
      ids.add(res.getBody().get("id").asText());
      if (res.getStatusCode() == HttpStatus.CREATED) {
        created++;
      }
    }
    pool.shutdown();

    assertThat(ids).hasSize(1);
    assertThat(created).isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE idempotency_key = 'race-key'", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void getReturnsJobWithAttemptHistory() {
    var job = enqueue(Map.of("type", "t")).getBody();
    String id = job.get("id").asText();
    jdbc.update(
        "INSERT INTO job_attempts (job_id, attempt_number, worker_id, outcome, error_message)"
            + " VALUES (?::uuid, 1, 'w1', 'FAILED_RETRYABLE', 'boom')",
        id);

    var res = http.getForEntity("/jobs/" + id, JsonNode.class);

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody().get("attempts")).hasSize(1);
    assertThat(res.getBody().get("attempts").get(0).get("errorMessage").asText()).isEqualTo("boom");
  }

  @Test
  void getUnknownJobIs404ProblemDetail() {
    var res = http.getForEntity("/jobs/" + java.util.UUID.randomUUID(), JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(res.getHeaders().getContentType().toString()).startsWith("application/problem+json");
  }

  @Test
  void malformedIdIs400() {
    var res = http.getForEntity("/jobs/not-a-uuid", JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void listFiltersAndPaginates() {
    for (int i = 0; i < 5; i++) {
      enqueue(Map.of("type", "email", "queue", "q1"));
    }
    enqueue(Map.of("type", "report", "queue", "q2"));

    var all = http.getForEntity("/jobs?size=4", JsonNode.class).getBody();
    assertThat(all.get("total").asInt()).isEqualTo(6);
    assertThat(all.get("items")).hasSize(4);

    var page2 = http.getForEntity("/jobs?size=4&page=1", JsonNode.class).getBody();
    assertThat(page2.get("items")).hasSize(2);

    var byType = http.getForEntity("/jobs?type=report", JsonNode.class).getBody();
    assertThat(byType.get("total").asInt()).isEqualTo(1);

    var byQueue = http.getForEntity("/jobs?queue=q1&status=PENDING", JsonNode.class).getBody();
    assertThat(byQueue.get("total").asInt()).isEqualTo(5);

    var none = http.getForEntity("/jobs?status=DEAD", JsonNode.class).getBody();
    assertThat(none.get("total").asInt()).isZero();
  }

  @Test
  void listRejectsBadParams() {
    assertThat(http.getForEntity("/jobs?status=BOGUS", JsonNode.class).getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(http.getForEntity("/jobs?size=1000", JsonNode.class).getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void cancelPendingJobSucceedsOnlyOnce() {
    String id = enqueue(Map.of("type", "t")).getBody().get("id").asText();

    var first = post("/jobs/" + id + "/cancel", null, null);
    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(first.getBody().get("status").asText()).isEqualTo("CANCELLED");
    assertThat(first.getBody().get("finishedAt").isNull()).isFalse();

    var second = post("/jobs/" + id + "/cancel", null, null);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
  }

  @Test
  void cancelRunningJobIsConflictAndLeavesItUntouched() {
    String id = enqueue(Map.of("type", "t")).getBody().get("id").asText();
    jdbc.update(
        "UPDATE jobs SET status='RUNNING', locked_by='w1', lease_expires_at = now() + interval '30 s'"
            + " WHERE id = ?::uuid",
        id);

    var res = post("/jobs/" + id + "/cancel", null, null);

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE id = ?::uuid", String.class, id))
        .isEqualTo("RUNNING");
  }

  @Test
  void cancelUnknownJobIs404() {
    var res = post("/jobs/" + java.util.UUID.randomUUID() + "/cancel", null, null);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  void openApiDocsArePublished() {
    var res = http.getForEntity("/v3/api-docs", JsonNode.class);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody().get("paths").has("/jobs")).isTrue();
    assertThat(res.getBody().get("paths").has("/jobs/{id}/cancel")).isTrue();
  }
}
