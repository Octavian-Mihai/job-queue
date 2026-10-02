package dev.jobqueue.config;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Top-level configuration. Roles come from {@code JOBQUEUE_ROLES=api,worker} (default: both). Later
 * phases attach their beans with {@code @ConditionalOnProperty} / {@link #hasRole(Role)}.
 */
@ConfigurationProperties("jobqueue")
public record JobQueueProperties(Set<Role> roles, String workerId) {

  public JobQueueProperties {
    roles = (roles == null || roles.isEmpty()) ? EnumSet.allOf(Role.class) : EnumSet.copyOf(roles);
    workerId = (workerId == null || workerId.isBlank()) ? defaultWorkerId() : workerId;
  }

  public boolean hasRole(Role role) {
    return roles.contains(role);
  }

  /** Hostname (the container id under Docker) plus a random suffix, unique per process start. */
  private static String defaultWorkerId() {
    String host;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      host = "unknown-host";
    }
    return host + "-" + UUID.randomUUID().toString().substring(0, 8);
  }
}
