package dev.jobqueue.core;

/** {@code created} is false when an existing job was returned for a repeated idempotency key. */
public record EnqueueResult(Job job, boolean created) {}
