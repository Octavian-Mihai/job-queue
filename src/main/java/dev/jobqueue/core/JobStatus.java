package dev.jobqueue.core;

import java.util.EnumSet;
import java.util.Set;

/**
 * Job lifecycle. {@link #canTransitionTo} is the documented state machine; the SQL guards ({@code
 * WHERE status = ...}) are what actually enforce it against concurrent writers.
 *
 * <pre>
 * PENDING -> RUNNING | CANCELLED
 * RUNNING -> SUCCEEDED | PENDING (retry / lease expired) | DEAD
 * SUCCEEDED, DEAD, CANCELLED are terminal (replaying a DEAD job creates a NEW job)
 * </pre>
 */
public enum JobStatus {
  PENDING,
  RUNNING,
  SUCCEEDED,
  DEAD,
  CANCELLED;

  public Set<JobStatus> allowedNext() {
    return switch (this) {
      case PENDING -> EnumSet.of(RUNNING, CANCELLED);
      case RUNNING -> EnumSet.of(SUCCEEDED, PENDING, DEAD);
      case SUCCEEDED, DEAD, CANCELLED -> EnumSet.noneOf(JobStatus.class);
    };
  }

  public boolean canTransitionTo(JobStatus next) {
    return allowedNext().contains(next);
  }

  public boolean isTerminal() {
    return allowedNext().isEmpty();
  }
}
