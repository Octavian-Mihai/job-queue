package dev.jobqueue.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.OffsetDateTime;

/**
 * A job to enqueue. At most one of {@code runAt} / {@code delaySeconds} is set; a delay is resolved
 * against the database clock so all API instances agree on "now".
 */
public record NewJob(
    String queueName,
    String type,
    JsonNode payload,
    int priority,
    int maxAttempts,
    OffsetDateTime runAt,
    long delaySeconds,
    String idempotencyKey) {}
