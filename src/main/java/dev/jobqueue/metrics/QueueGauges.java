package dev.jobqueue.metrics;

import dev.jobqueue.core.JobStatus;
import dev.jobqueue.core.QueueStats;
import dev.jobqueue.core.StatsRepository;
import dev.jobqueue.worker.InFlightJobs;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Queue-level gauges, read from the database when Prometheus scrapes.
 *
 * <p>Every instance exposes the same queue-wide values, so dashboards aggregate with {@code max()},
 * never {@code sum()}. The snapshot is cached for a couple of seconds so several gauges (and
 * several scrapers) cost one round trip, not one each.
 */
@Component
public class QueueGauges {

  private static final Duration TTL = Duration.ofSeconds(2);

  private final StatsRepository stats;
  private QueueStats cached;
  private long cachedAtNanos;

  public QueueGauges(StatsRepository stats, MeterRegistry registry, InFlightJobs inFlight) {
    this.stats = stats;
    for (JobStatus status : JobStatus.values()) {
      Gauge.builder("jobqueue.jobs", () -> snapshot().counts().get(status))
          .description("Jobs by status (queue-wide: aggregate instances with max())")
          .tag("status", status.name())
          .register(registry);
    }
    Gauge.builder("jobqueue.oldest.pending.age", () -> snapshot().oldestPendingAgeSeconds())
        .description("Age of the longest-waiting runnable pending job")
        .baseUnit("seconds")
        .register(registry);
    Gauge.builder("jobqueue.dlq.size", () -> snapshot().deadLetterQueueSize())
        .description("Dead letters not yet replayed")
        .register(registry);
    Gauge.builder("jobqueue.jobs.in.flight", inFlight, InFlightJobs::count)
        .description("Jobs executing in this process (aggregate instances with sum())")
        .register(registry);
  }

  private synchronized QueueStats snapshot() {
    long now = System.nanoTime();
    if (cached == null || now - cachedAtNanos > TTL.toNanos()) {
      try {
        cached = stats.snapshot();
        cachedAtNanos = now;
      } catch (RuntimeException e) {
        if (cached == null) {
          throw e;
        }
        // DB hiccup: keep serving the last good snapshot rather than failing the scrape.
      }
    }
    return cached;
  }

  /** For tests. */
  Supplier<QueueStats> supplier() {
    return this::snapshot;
  }
}
