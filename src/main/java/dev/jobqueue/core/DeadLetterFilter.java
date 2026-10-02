package dev.jobqueue.core;

/** Optional filters shared by DLQ listing and bulk replay; null means "any". */
public record DeadLetterFilter(String type, String queue, String reason, Boolean replayed) {}
