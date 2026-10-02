package dev.jobqueue.worker;

/** Why a job entered the dead-letter queue (stored in {@code dead_letter_jobs.reason}). */
public final class DeadReason {
  public static final String NON_RETRYABLE = "NON_RETRYABLE";
  public static final String MAX_ATTEMPTS_EXCEEDED = "MAX_ATTEMPTS_EXCEEDED";

  private DeadReason() {}
}
