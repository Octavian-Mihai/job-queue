package dev.jobqueue.core;

public class JobStateConflictException extends RuntimeException {
  public JobStateConflictException(String message) {
    super(message);
  }
}
