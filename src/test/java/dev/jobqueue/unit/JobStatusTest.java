package dev.jobqueue.unit;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jobqueue.core.JobStatus;
import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class JobStatusTest {

  @Test
  void allowedTransitionsMatchTheDocumentedStateMachine() {
    assertThat(JobStatus.PENDING.allowedNext())
        .containsExactlyInAnyOrder(JobStatus.RUNNING, JobStatus.CANCELLED);
    assertThat(JobStatus.RUNNING.allowedNext())
        .containsExactlyInAnyOrder(JobStatus.SUCCEEDED, JobStatus.PENDING, JobStatus.DEAD);
  }

  @Test
  void terminalStatesGoNowhere() {
    for (JobStatus s : EnumSet.of(JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED)) {
      assertThat(s.isTerminal()).isTrue();
      for (JobStatus next : JobStatus.values()) {
        assertThat(s.canTransitionTo(next)).as("%s -> %s", s, next).isFalse();
      }
    }
  }

  @Test
  void illegalShortcutsAreRejected() {
    assertThat(JobStatus.PENDING.canTransitionTo(JobStatus.SUCCEEDED)).isFalse();
    assertThat(JobStatus.PENDING.canTransitionTo(JobStatus.DEAD)).isFalse();
    assertThat(JobStatus.RUNNING.canTransitionTo(JobStatus.CANCELLED)).isFalse();
    assertThat(JobStatus.RUNNING.canTransitionTo(JobStatus.RUNNING)).isFalse();
  }
}
