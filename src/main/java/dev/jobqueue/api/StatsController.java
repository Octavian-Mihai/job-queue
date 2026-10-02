package dev.jobqueue.api;

import dev.jobqueue.config.ConditionalOnRole;
import dev.jobqueue.config.Role;
import dev.jobqueue.core.JobStatus;
import dev.jobqueue.core.StatsRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnRole(Role.API)
@Tag(name = "Stats")
public class StatsController {

  public record StatsResponse(
      Map<String, Long> countsByStatus,
      long total,
      double oldestPendingAgeSeconds,
      long deadLetterQueueSize) {}

  private final StatsRepository stats;

  public StatsController(StatsRepository stats) {
    this.stats = stats;
  }

  @GetMapping("/stats")
  @Operation(summary = "Counts by status and the age of the oldest runnable pending job")
  public StatsResponse stats() {
    var snapshot = stats.snapshot();
    Map<String, Long> counts = new LinkedHashMap<>();
    long total = 0;
    for (JobStatus s : JobStatus.values()) {
      long n = snapshot.counts().get(s);
      counts.put(s.name(), n);
      total += n;
    }
    return new StatsResponse(
        counts, total, snapshot.oldestPendingAgeSeconds(), snapshot.deadLetterQueueSize());
  }
}
