package dev.jobqueue.core;

import java.util.UUID;

public class JobNotFoundException extends RuntimeException {
  public JobNotFoundException(UUID id) {
    super("Job " + id + " not found");
  }
}
