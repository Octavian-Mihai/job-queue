package dev.jobqueue.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jobqueue.worker.WorkerProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class WorkerPropertiesTest {

  private static WorkerProperties props(Duration lease, Duration heartbeat) {
    return new WorkerProperties(0, 0, null, null, lease, heartbeat, null, 0, null, null);
  }

  @Test
  void heartbeatDefaultsToAThirdOfTheLease() {
    assertThat(props(Duration.ofSeconds(30), null).heartbeatInterval())
        .isEqualTo(Duration.ofSeconds(10));
    assertThat(props(null, null).heartbeatInterval()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void heartbeatMustBeShorterThanTheLease() {
    assertThatThrownBy(() -> props(Duration.ofSeconds(5), Duration.ofSeconds(5)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("heartbeat");
    assertThatThrownBy(() -> props(Duration.ofSeconds(5), Duration.ofSeconds(9)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void sensibleDefaults() {
    var p = props(null, null);
    assertThat(p.concurrency()).isEqualTo(8);
    assertThat(p.reaperInterval()).isEqualTo(Duration.ofSeconds(5));
    assertThat(p.reaperBatchSize()).isEqualTo(100);
    assertThat(p.shutdownGracePeriod()).isEqualTo(Duration.ofSeconds(30));
  }
}
