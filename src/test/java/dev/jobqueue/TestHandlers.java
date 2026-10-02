package dev.jobqueue;

import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import dev.jobqueue.handler.NonRetryableException;
import dev.jobqueue.handler.RetryableException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/** Test-only handlers (picked up by component scan in test contexts). */
public final class TestHandlers {

  private TestHandlers() {}

  /** Records executions and detects a job running concurrently with itself. */
  @Component
  public static class Probe implements JobHandler {
    public final Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();
    private final Map<UUID, AtomicInteger> inFlight = new ConcurrentHashMap<>();
    public final AtomicInteger overlaps = new AtomicInteger();
    public final AtomicInteger peakParallel = new AtomicInteger();
    private final AtomicInteger current = new AtomicInteger();

    public void reset() {
      executions.clear();
      inFlight.clear();
      overlaps.set(0);
      peakParallel.set(0);
      current.set(0);
    }

    @Override
    public String type() {
      return "test-probe";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
      executions.computeIfAbsent(ctx.jobId(), k -> new AtomicInteger()).incrementAndGet();
      if (inFlight.computeIfAbsent(ctx.jobId(), k -> new AtomicInteger()).incrementAndGet() > 1) {
        overlaps.incrementAndGet();
      }
      int now = current.incrementAndGet();
      peakParallel.accumulateAndGet(now, Math::max);
      try {
        Thread.sleep(10);
      } finally {
        current.decrementAndGet();
        inFlight.get(ctx.jobId()).decrementAndGet();
      }
    }
  }

  /**
   * Fails the first {@code failFirst} attempts. {@code kind}: retryable (default), unknown (plain
   * RuntimeException) or permanent (NonRetryableException, on every attempt).
   */
  @Component
  public static class Flaky implements JobHandler {
    @Override
    public String type() {
      return "test-flaky";
    }

    @Override
    public void handle(JobContext ctx) {
      String kind = ctx.payload().path("kind").asText("retryable");
      if (kind.equals("permanent")) {
        throw new NonRetryableException("bad input, never retry");
      }
      if (ctx.attempt() <= ctx.payload().path("failFirst").asInt(0)) {
        if (kind.equals("unknown")) {
          throw new IllegalStateException("surprise on attempt " + ctx.attempt());
        }
        throw new RetryableException("transient on attempt " + ctx.attempt());
      }
    }
  }

  /** Fails while the gate is closed; succeeds once a test opens it (models "fix, then replay"). */
  @Component
  public static class Gate implements JobHandler {
    public final java.util.concurrent.atomic.AtomicBoolean open =
        new java.util.concurrent.atomic.AtomicBoolean();

    @Override
    public String type() {
      return "test-gate";
    }

    @Override
    public void handle(JobContext ctx) {
      if (!open.get()) {
        throw new RetryableException("downstream still broken");
      }
    }
  }

  /** Blocks (interruptibly) far longer than any test timeout. */
  @Component
  public static class Hang implements JobHandler {
    @Override
    public String type() {
      return "test-hang";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
      Thread.sleep(60_000);
    }
  }
}
