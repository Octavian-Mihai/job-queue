package dev.jobqueue.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;
import java.util.UUID;

/** A job as it was when it died, plus replay bookkeeping. */
public record DeadLetter(
    UUID id,
    UUID jobId,
    String queueName,
    String type,
    JsonNode payload,
    int priority,
    int attempts,
    int maxAttempts,
    String lastError,
    String reason,
    OffsetDateTime jobCreatedAt,
    OffsetDateTime deadAt,
    OffsetDateTime replayedAt,
    UUID replayedJobId) {}
