package dev.jobqueue.worker;

import dev.jobqueue.handler.NonRetryableException;
import dev.jobqueue.handler.RetryableException;

/**
 * Decides whether a handler failure is worth retrying. Pure.
 *
 * <p>The nearest classified exception in the cause chain wins, so a handler that wraps a {@link
 * NonRetryableException} in something generic is still non-retryable, while an explicit {@link
 * RetryableException} on top overrides whatever it wraps. <b>Anything unrecognised is
 * retryable</b>: dead-lettering a job because of a bug we did not anticipate would lose work that a
 * later attempt, or a deploy, may well complete.
 */
public final class FailureClassifier {

  public record Classification(boolean retryable, String outcome) {}

  static final Classification RETRYABLE = new Classification(true, "FAILED_RETRYABLE");
  static final Classification NON_RETRYABLE = new Classification(false, "FAILED_NON_RETRYABLE");

  private static final int MAX_CAUSE_DEPTH = 16;

  private FailureClassifier() {}

  public static Classification classify(Throwable error) {
    Throwable t = error;
    for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause()) {
      if (t instanceof NonRetryableException) {
        return NON_RETRYABLE;
      }
      if (t instanceof RetryableException) {
        return RETRYABLE;
      }
    }
    return RETRYABLE;
  }
}
