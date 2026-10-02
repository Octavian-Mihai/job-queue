package dev.jobqueue.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jobqueue.retry.BackoffPolicy;
import java.time.Duration;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

  private final BackoffPolicy policy =
      new BackoffPolicy(Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30));

  @Test
  void ceilingGrowsExponentiallyThenIsCapped() {
    assertThat(policy.ceiling(1)).isEqualTo(Duration.ofSeconds(1));
    assertThat(policy.ceiling(2)).isEqualTo(Duration.ofSeconds(2));
    assertThat(policy.ceiling(3)).isEqualTo(Duration.ofSeconds(4));
    assertThat(policy.ceiling(5)).isEqualTo(Duration.ofSeconds(16));
    assertThat(policy.ceiling(6)).isEqualTo(Duration.ofSeconds(30)); // 32s capped
    assertThat(policy.ceiling(50)).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void hugeAttemptCountsDoNotOverflow() {
    assertThat(policy.ceiling(10_000)).isEqualTo(Duration.ofSeconds(30));
    assertThat(policy.ceiling(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void fractionalMultiplierWorks() {
    var p = new BackoffPolicy(Duration.ofSeconds(10), 1.5, Duration.ofMinutes(10));
    assertThat(p.ceiling(1)).isEqualTo(Duration.ofSeconds(10));
    assertThat(p.ceiling(2)).isEqualTo(Duration.ofSeconds(15));
    assertThat(p.ceiling(3)).isEqualTo(Duration.ofMillis(22_500));
  }

  @Test
  void multiplierOfOneGivesConstantCeiling() {
    var p = new BackoffPolicy(Duration.ofSeconds(3), 1.0, Duration.ofMinutes(1));
    assertThat(p.ceiling(1)).isEqualTo(p.ceiling(9));
  }

  @Test
  void fullJitterSpansZeroToCeiling() {
    // nextLong(bound) returns [0, bound): the policy passes ceiling+1 so the ceiling is reachable.
    assertThat(policy.delay(3, new FixedRandom(0))).isEqualTo(Duration.ZERO);
    assertThat(policy.delay(3, new FixedRandom(Long.MAX_VALUE))).isEqualTo(Duration.ofSeconds(4));
  }

  @Test
  void delaysAlwaysFallWithinBoundsAndActuallyVary() {
    var rnd = new java.util.Random(42);
    var seen = new java.util.HashSet<Long>();
    for (int attempt = 1; attempt <= 12; attempt++) {
      for (int i = 0; i < 500; i++) {
        Duration d = policy.delay(attempt, rnd);
        assertThat(d).isBetween(Duration.ZERO, policy.ceiling(attempt));
        if (attempt == 4) {
          seen.add(d.toMillis());
        }
      }
    }
    assertThat(seen.size()).as("jitter should produce many distinct delays").isGreaterThan(100);
  }

  @Test
  void rejectsInvalidConfiguration() {
    assertThatThrownBy(() -> new BackoffPolicy(Duration.ZERO, 2, Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(1), 0.5, Duration.ofSeconds(5)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(5), 2, Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> policy.ceiling(0)).isInstanceOf(IllegalArgumentException.class);
  }

  private static class FixedRandom implements RandomGenerator {
    private final long value;

    FixedRandom(long value) {
      this.value = value;
    }

    @Override
    public long nextLong() {
      return value;
    }

    @Override
    public long nextLong(long bound) {
      return Math.min(value, bound - 1);
    }
  }
}
