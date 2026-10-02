package dev.jobqueue.retry;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code jobqueue.retry.defaults.*} apply to every type; {@code jobqueue.retry.types.<type>.*}
 * override individual fields for one job type.
 */
@ConfigurationProperties("jobqueue.retry")
public record RetryProperties(RetrySettings defaults, Map<String, RetrySettings> types) {

  public RetryProperties {
    defaults = defaults == null ? new RetrySettings(null, null, null, null, null) : defaults;
    types = types == null ? Map.of() : Map.copyOf(types);
  }
}
