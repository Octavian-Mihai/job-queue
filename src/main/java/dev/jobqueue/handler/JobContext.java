package dev.jobqueue.handler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/**
 * What a handler sees. {@code jobId} is stable across attempts: use it as the dedupe key for side
 * effects, because delivery is at-least-once and a job can run more than once.
 */
public record JobContext(
    UUID jobId, String type, int attempt, int maxAttempts, JsonNode payload, String workerId) {}
