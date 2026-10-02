package dev.jobqueue.handler.demo;

import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import dev.jobqueue.handler.NonRetryableException;
import dev.jobqueue.handler.RetryableException;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Simulated email sender, and the project's demonstration of an <b>idempotent handler</b>.
 *
 * <p>Delivery is at-least-once: a worker can finish the work and die before recording success, so
 * the job runs again. The "send" here is an {@code INSERT ... ON CONFLICT DO NOTHING} into {@code
 * email_outbox} keyed by the job id, so a re-run finds the effect already present and sends
 * nothing. A real handler would pass the same key to its provider (e.g. an idempotency header).
 *
 * <p>Payload: {@code {"to": "...", "subject": "...", "simulate": "random|ok|transient|after-send",
 * "failRate": 0.2}}. {@code after-send} performs the send and then fails, i.e. the lost-ack case.
 */
@Component
public class SendEmailHandler implements JobHandler {

  private static final Logger log = LoggerFactory.getLogger(SendEmailHandler.class);

  private final JdbcTemplate jdbc;

  public SendEmailHandler(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public String type() {
    return "send-email";
  }

  @Override
  public void handle(JobContext ctx) throws Exception {
    String to = ctx.payload().path("to").asText("");
    if (!to.contains("@")) {
      throw new NonRetryableException("invalid recipient '" + to + "'");
    }
    String mode = ctx.payload().path("simulate").asText("random");
    double failRate = ctx.payload().path("failRate").asDouble(0.2);
    Thread.sleep(ThreadLocalRandom.current().nextLong(20, 80)); // pretend SMTP latency

    boolean failBeforeSend =
        mode.equals("transient")
            || (mode.equals("random") && ThreadLocalRandom.current().nextDouble() < failRate);
    if (failBeforeSend) {
      throw new RetryableException("SMTP 421 service temporarily unavailable");
    }

    int inserted =
        jdbc.update(
            "INSERT INTO email_outbox (dedupe_key, recipient, subject) VALUES (?, ?, ?)"
                + " ON CONFLICT (dedupe_key) DO NOTHING",
            ctx.jobId().toString(),
            to,
            ctx.payload().path("subject").asText(null));
    if (inserted == 0) {
      log.info("email already sent for job {}, skipping duplicate delivery", ctx.jobId());
    }

    if (mode.equals("after-send")) {
      throw new RetryableException("connection reset after send (ack lost)");
    }
  }
}
