package dev.jobqueue.retry;

import java.time.Duration;
import org.springframework.stereotype.Component;

/** Resolves the effective retry policy for a job type: per-type overrides over defaults. */
@Component
public class RetryPolicies {

  private static final Duration DEFAULT_BASE = Duration.ofSeconds(2);
  private static final double DEFAULT_MULTIPLIER = 2.0;
  private static final Duration DEFAULT_MAX_DELAY = Duration.ofMinutes(5);
  private static final int DEFAULT_MAX_ATTEMPTS = 5;
  private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

  /** The effective policy for one job type. */
  public record Resolved(BackoffPolicy backoff, int maxAttempts, Duration executionTimeout) {}

  private final RetryProperties props;

  public RetryPolicies(RetryProperties props) {
    this.props = props;
    // Fail at startup, not at the first failed job, if any merged policy is invalid
    // (e.g. a per-type base-delay larger than the inherited max-delay).
    resolve("(defaults)");
    props.types().keySet().forEach(this::resolve);
  }

  public Resolved resolve(String type) {
    RetrySettings d = props.defaults();
    RetrySettings t =
        props.types().getOrDefault(type, new RetrySettings(null, null, null, null, null));
    int maxAttempts = pick(t.maxAttempts(), d.maxAttempts(), DEFAULT_MAX_ATTEMPTS);
    if (maxAttempts < 1) {
      throw new IllegalStateException("Invalid retry config for '" + type + "': max-attempts < 1");
    }
    try {
      return new Resolved(
          new BackoffPolicy(
              pick(t.baseDelay(), d.baseDelay(), DEFAULT_BASE),
              pick(t.multiplier(), d.multiplier(), DEFAULT_MULTIPLIER),
              pick(t.maxDelay(), d.maxDelay(), DEFAULT_MAX_DELAY)),
          maxAttempts,
          pick(t.executionTimeout(), d.executionTimeout(), DEFAULT_TIMEOUT));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Invalid retry config for '" + type + "': " + e.getMessage(), e);
    }
  }

  private static <T> T pick(T perType, T defaults, T fallback) {
    return perType != null ? perType : defaults != null ? defaults : fallback;
  }
}
