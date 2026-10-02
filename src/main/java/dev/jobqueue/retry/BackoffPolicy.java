package dev.jobqueue.retry;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with <b>full jitter</b> and a cap. Pure: no clock, no I/O; the randomness is
 * injected so tests are deterministic.
 *
 * <p>The ceiling after the n-th failed attempt is {@code min(maxDelay, baseDelay *
 * multiplier^(n-1))}; the actual delay is uniform in {@code [0, ceiling]}. Full jitter spreads
 * retries of jobs that failed together (e.g. a downstream outage) across the whole window instead
 * of re-hitting the recovering service in synchronized waves. Trade-off: an individual retry can be
 * almost immediate.
 */
public record BackoffPolicy(Duration baseDelay, double multiplier, Duration maxDelay) {

  public BackoffPolicy {
    if (baseDelay.isNegative() || baseDelay.isZero()) {
      throw new IllegalArgumentException("baseDelay must be positive");
    }
    if (multiplier < 1.0) {
      throw new IllegalArgumentException("multiplier must be >= 1");
    }
    if (maxDelay.compareTo(baseDelay) < 0) {
      throw new IllegalArgumentException("maxDelay must be >= baseDelay");
    }
  }

  /** Upper bound of the delay after {@code attempt} (1-based) failed. */
  public Duration ceiling(int attempt) {
    if (attempt < 1) {
      throw new IllegalArgumentException("attempt is 1-based");
    }
    double ceilingMs = baseDelay.toMillis() * Math.pow(multiplier, attempt - 1.0);
    // Math.pow overflows to Infinity for huge attempt counts; comparing as double handles that.
    return ceilingMs >= maxDelay.toMillis() ? maxDelay : Duration.ofMillis((long) ceilingMs);
  }

  /** Random delay in {@code [0, ceiling(attempt)]}. */
  public Duration delay(int attempt, RandomGenerator random) {
    long ceilingMs = ceiling(attempt).toMillis();
    return Duration.ofMillis(random.nextLong(ceilingMs + 1));
  }
}
