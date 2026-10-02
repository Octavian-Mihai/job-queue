package dev.jobqueue.handler.demo;

import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import dev.jobqueue.handler.NonRetryableException;
import dev.jobqueue.handler.RetryableException;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * Simulated HTTP webhook delivery (no network). 5xx / timeouts are transient and retryable; 4xx
 * means the receiver rejected the request for good, so it is non-retryable.
 *
 * <p>Payload: {@code {"url": "...", "simulate": "random|ok|transient|permanent"}}. In {@code
 * random} mode: 70% success, 20% transient (503 or timeout), 10% permanent (400 or 410).
 */
@Component
public class DeliverWebhookHandler implements JobHandler {

  @Override
  public String type() {
    return "deliver-webhook";
  }

  @Override
  public void handle(JobContext ctx) throws Exception {
    String url = ctx.payload().path("url").asText("");
    if (url.isBlank()) {
      throw new NonRetryableException("payload.url is required");
    }
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    Thread.sleep(rnd.nextLong(20, 80)); // pretend network latency

    String mode = ctx.payload().path("simulate").asText("random");
    if (mode.equals("random")) {
      double roll = rnd.nextDouble();
      mode = roll < 0.70 ? "ok" : roll < 0.90 ? "transient" : "permanent";
    }
    switch (mode) {
      case "ok" -> {}
      case "transient" -> {
        if (rnd.nextBoolean()) {
          throw new RetryableException("HTTP 503 from " + url);
        }
        throw new RetryableException("timeout calling " + url);
      }
      case "permanent" ->
          throw new NonRetryableException(
              "HTTP " + (rnd.nextBoolean() ? 400 : 410) + " from " + url);
      default -> throw new NonRetryableException("unknown simulate mode '" + mode + "'");
    }
  }
}
