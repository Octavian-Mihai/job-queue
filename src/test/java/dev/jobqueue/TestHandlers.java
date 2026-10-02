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

  /**
   * Sleeps {@code sleepMs}. With {@code firstAttemptOnly} later attempts return immediately.
   * Records which attempts were interrupted and which started.
   */
  @Component
  public static class Sleeper implements JobHandler {
    public final java.util.Set<String> started = ConcurrentHashMap.newKeySet();
    public final java.util.List<String> interrupted =
        new java.util.concurrent.CopyOnWriteArrayList<>();

    public void reset() {
      started.clear();
      interrupted.clear();
    }

    public static String key(UUID jobId, int attempt) {
      return jobId + ":" + attempt;
    }

    @Override
    public String type() {
      return "test-sleeper";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
      started.add(key(ctx.jobId(), ctx.attempt()));
      if (ctx.payload().path("firstAttemptOnly").asBoolean(false) && ctx.attempt() > 1) {
        return;
      }
      try {
        Thread.sleep(ctx.payload().path("sleepMs").asLong(2_000));
      } catch (InterruptedException e) {
        interrupted.add(key(ctx.jobId(), ctx.attempt()));
        throw e;
      }
    }
  }

  /** Ignores interruption on its first attempt (a non-cooperative handler). */
  @Component
  public static class Stubborn implements JobHandler {
    @Override
    public String type() {
      return "test-stubborn";
    }

    @Override
    public void handle(JobContext ctx) {
      if (ctx.attempt() > 1) {
        return;
      }
      long end = System.currentTimeMillis() + ctx.payload().path("stubbornMs").asLong(4_000);
      while (System.currentTimeMillis() < end) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException ignored) {
          // refuses to stop
        }
      }
    }
  }
}
