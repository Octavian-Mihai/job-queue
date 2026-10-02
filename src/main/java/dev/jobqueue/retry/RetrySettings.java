package dev.jobqueue.retry;

import java.time.Duration;

/** Retry knobs as configured; any null field falls back to the default settings. */
public record RetrySettings(
    Duration baseDelay,
    Double multiplier,
    Duration maxDelay,
    Integer maxAttempts,
    Duration executionTimeout) {}
