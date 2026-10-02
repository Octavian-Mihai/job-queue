package dev.jobqueue.worker;

import java.util.UUID;

/** Identifies one claim of a job: the job and the attempt number it was claimed for. */
public record Lease(UUID jobId, int attempt) {}
