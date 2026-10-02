package dev.jobqueue.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Job(
    UUID id,
    String queueName,
    String type,
    JsonNode payload,
    JobStatus status,
    int priority,
    int attempts,
    int maxAttempts,
    OffsetDateTime runAt,
    String lockedBy,
    OffsetDateTime leaseExpiresAt,
    String lastError,
    String idempotencyKey,
    OffsetDateTime createdAt,
    OffsetDateTime updatedAt,
    OffsetDateTime finishedAt) {}
