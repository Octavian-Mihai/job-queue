package dev.jobqueue.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.jobqueue.retry.RetryPolicies;
import dev.jobqueue.retry.RetryProperties;
import dev.jobqueue.retry.RetrySettings;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RetryPoliciesTest {

  @Test
  void builtInDefaultsApplyWhenNothingIsConfigured() {
    var p = new RetryPolicies(new RetryProperties(null, null)).resolve("anything");
    assertThat(p.maxAttempts()).isEqualTo(5);
    assertThat(p.backoff().baseDelay()).isEqualTo(Duration.ofSeconds(2));
    assertThat(p.backoff().multiplier()).isEqualTo(2.0);
    assertThat(p.backoff().maxDelay()).isEqualTo(Duration.ofMinutes(5));
    assertThat(p.executionTimeout()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void perTypeSettingsOverrideOnlyTheFieldsTheySet() {
    var defaults = new RetrySettings(Duration.ofSeconds(1), 3.0, Duration.ofMinutes(1), 7, null);
    var hook = new RetrySettings(null, null, Duration.ofSeconds(10), 9, Duration.ofSeconds(30));
    var policies = new RetryPolicies(new RetryProperties(defaults, Map.of("hook", hook)));

    var other = policies.resolve("other");
    assertThat(other.maxAttempts()).isEqualTo(7);
    assertThat(other.backoff().maxDelay()).isEqualTo(Duration.ofMinutes(1));

    var h = policies.resolve("hook");
    assertThat(h.maxAttempts()).isEqualTo(9);
    assertThat(h.backoff().baseDelay()).isEqualTo(Duration.ofSeconds(1)); // inherited
    assertThat(h.backoff().multiplier()).isEqualTo(3.0); // inherited
    assertThat(h.backoff().maxDelay()).isEqualTo(Duration.ofSeconds(10)); // overridden
    assertThat(h.executionTimeout()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void invalidMaxAttemptsIsRejected() {
    var bad = new RetrySettings(null, null, null, 0, null);
    assertThatThrownBy(() -> new RetryPolicies(new RetryProperties(bad, null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("max-attempts");
  }

  @Test
  void inconsistentMergedConfigFailsAtStartupNamingTheType() {
    var defaults =
        new RetrySettings(Duration.ofMillis(50), null, Duration.ofMillis(100), null, null);
    var slowBase = new RetrySettings(Duration.ofSeconds(5), null, null, null, null);
    var props = new RetryProperties(defaults, Map.of("generate-report", slowBase));
    assertThatThrownBy(() -> new RetryPolicies(props))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("generate-report")
        .hasMessageContaining("maxDelay");
  }
}
