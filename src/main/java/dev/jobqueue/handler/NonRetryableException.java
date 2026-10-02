package dev.jobqueue.handler;

/** Permanent failure: retrying cannot help, so the job goes straight to the dead-letter queue. */
public class NonRetryableException extends RuntimeException {
  public NonRetryableException(String message) {
    super(message);
  }

  public NonRetryableException(String message, Throwable cause) {
    super(message, cause);
  }
}
