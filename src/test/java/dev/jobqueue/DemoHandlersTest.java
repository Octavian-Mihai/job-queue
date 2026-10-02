package dev.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jobqueue.handler.HandlerRegistry;
import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import dev.jobqueue.handler.NonRetryableException;
import dev.jobqueue.handler.RetryableException;
import dev.jobqueue.handler.demo.SendEmailHandler;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class DemoHandlersTest extends PostgresTestBase {

  @Autowired SendEmailHandler email;
  @Autowired HandlerRegistry registry;
  @Autowired JdbcTemplate jdbc;
  @Autowired ObjectMapper json;

  @BeforeEach
  void clean() {
    jdbc.update("DELETE FROM email_outbox");
  }

  private JobContext ctx(UUID jobId, Map<String, Object> payload) {
    JsonNode node = json.valueToTree(payload);
    return new JobContext(jobId, "send-email", 1, 3, node, "w");
  }

  @Test
  void rerunningAnEmailJobDoesNotSendTwice() throws Exception {
    UUID jobId = UUID.randomUUID();
    var payload = Map.<String, Object>of("to", "a@b.c", "simulate", "ok");

    email.handle(ctx(jobId, payload));
    email.handle(ctx(jobId, payload)); // at-least-once: the same job delivered again

    assertThat(jdbc.queryForObject("SELECT count(*) FROM email_outbox", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void lostAckAfterSendStillLeavesExactlyOneEmail() {
    UUID jobId = UUID.randomUUID();
    var payload = Map.<String, Object>of("to", "a@b.c", "simulate", "after-send");

    for (int attempt = 0; attempt < 3; attempt++) {
      assertThatThrownBy(() -> email.handle(ctx(jobId, payload)))
          .isInstanceOf(RetryableException.class);
    }

    assertThat(jdbc.queryForObject("SELECT count(*) FROM email_outbox", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void transientFailureSendsNothing() {
    var payload = Map.<String, Object>of("to", "a@b.c", "simulate", "transient");
    assertThatThrownBy(() -> email.handle(ctx(UUID.randomUUID(), payload)))
        .isInstanceOf(RetryableException.class);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM email_outbox", Integer.class)).isZero();
  }

  @Test
  void invalidRecipientIsNonRetryable() {
    assertThatThrownBy(() -> email.handle(ctx(UUID.randomUUID(), Map.of("to", "nope"))))
        .isInstanceOf(NonRetryableException.class);
  }

  @Test
  void registryKnowsDemoHandlers() {
    assertThat(registry.types()).contains("send-email", "generate-report", "deliver-webhook");
  }

  @Test
  void registryRejectsDuplicateTypes() {
    JobHandler dup =
        new JobHandler() {
          public String type() {
            return "send-email";
          }

          public void handle(JobContext c) {}
        };
    assertThatThrownBy(() -> new HandlerRegistry(List.of(email, dup)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate handler");
  }
}
