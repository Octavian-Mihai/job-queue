package dev.jobqueue.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.stereotype.Component;

/** Handlers keyed by job type. Duplicate types fail fast at startup. */
@Component
public class HandlerRegistry {

  private final Map<String, JobHandler> byType = new HashMap<>();

  public HandlerRegistry(List<JobHandler> handlers) {
    for (JobHandler h : handlers) {
      JobHandler previous = byType.put(h.type(), h);
      if (previous != null) {
        throw new IllegalStateException(
            "Duplicate handler for job type '%s': %s and %s"
                .formatted(h.type(), previous.getClass().getName(), h.getClass().getName()));
      }
    }
  }

  public Optional<JobHandler> find(String type) {
    return Optional.ofNullable(byType.get(type));
  }

  public boolean isKnown(String type) {
    return byType.containsKey(type);
  }

  public Set<String> types() {
    return new TreeSet<>(byType.keySet());
  }
}
