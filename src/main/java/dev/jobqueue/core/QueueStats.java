package dev.jobqueue.core;

import java.util.Map;

/**
 * Point-in-time queue summary.
 *
 * @param counts job count per status (every status present, zero if none)
 * @param oldestPendingAgeSeconds how long the longest-waiting runnable PENDING job has waited (0 if
 *     none); the key "is the queue keeping up?" signal
 * @param deadLetterQueueSize dead letters not yet replayed
 */
public record QueueStats(
    Map<JobStatus, Long> counts, double oldestPendingAgeSeconds, long deadLetterQueueSize) {}
