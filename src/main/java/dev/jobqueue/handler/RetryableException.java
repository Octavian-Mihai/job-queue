package dev.jobqueue.handler;

/** Transient failure: the attempt failed but a later attempt may succeed. */
public class RetryableException extends RuntimeException {
  public RetryableException(String message) {
    super(message);
  }

  public RetryableException(String message, Throwable cause) {
    super(message, cause);
  }
}
