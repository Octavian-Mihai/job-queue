package dev.jobqueue.core;

import java.util.Set;

public class UnknownJobTypeException extends RuntimeException {
  public UnknownJobTypeException(String type, Set<String> known) {
    super("Unknown job type '" + type + "'; known types: " + known);
  }
}
