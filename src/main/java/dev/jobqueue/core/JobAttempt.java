package dev.jobqueue.core;

import java.time.OffsetDateTime;

public record JobAttempt(
    int attemptNumber,
    String workerId,
    OffsetDateTime startedAt,
    OffsetDateTime finishedAt,
    String outcome,
    String errorMessage,
    String stackTrace) {}
