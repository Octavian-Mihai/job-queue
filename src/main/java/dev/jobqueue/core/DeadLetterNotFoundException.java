package dev.jobqueue.core;

import java.util.UUID;

public class DeadLetterNotFoundException extends RuntimeException {
  public DeadLetterNotFoundException(UUID id) {
    super("Dead letter " + id + " not found");
  }
}
