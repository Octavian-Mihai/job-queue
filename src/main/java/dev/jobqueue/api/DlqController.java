package dev.jobqueue.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import dev.jobqueue.config.ConditionalOnRole;
import dev.jobqueue.config.Role;
import dev.jobqueue.core.DeadLetter;
import dev.jobqueue.core.DeadLetterFilter;
import dev.jobqueue.core.DeadLetterService;
import dev.jobqueue.core.JobAttempt;
import dev.jobqueue.core.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dlq")
@ConditionalOnRole(Role.API)
@Tag(name = "Dead-letter queue")
public class DlqController {

  /** A dead letter plus the full attempt history of the job that died. */
  public record DeadLetterDetail(@JsonUnwrapped DeadLetter entry, List<JobAttempt> attempts) {}

  public record ReplayResponse(int replayed, List<UUID> jobIds) {}

  private final DeadLetterService service;

  public DlqController(DeadLetterService service) {
    this.service = service;
  }

  @GetMapping
  @Operation(summary = "List dead-lettered jobs, newest first")
  public PageResult<DeadLetter> list(
      @RequestParam(required = false) String type,
      @RequestParam(required = false) String queue,
      @RequestParam(required = false) String reason,
      @RequestParam(required = false) Boolean replayed,
      @RequestParam(defaultValue = "0") @Min(0) int page,
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
    return service.list(new DeadLetterFilter(type, queue, reason, replayed), page, size);
  }

  @GetMapping("/{id}")
  @Operation(summary = "Inspect a dead letter with the job's full attempt history")
  public DeadLetterDetail get(@PathVariable UUID id) {
    DeadLetter entry = service.get(id);
    return new DeadLetterDetail(entry, service.attempts(entry));
  }

  @PostMapping("/{id}/replay")
  @Operation(
      summary = "Replay one dead letter",
      description =
          "Creates a NEW job (fresh attempt cycle) and records replayed_at. 409 if already"
              + " replayed.")
  public ResponseEntity<ReplayResponse> replayOne(@PathVariable UUID id) {
    UUID jobId = service.replayOne(id);
    return ResponseEntity.created(URI.create("/jobs/" + jobId))
        .body(new ReplayResponse(1, List.of(jobId)));
  }

  @PostMapping("/replay")
  @Operation(
      summary = "Replay all not-yet-replayed dead letters matching the filters",
      description = "Replays at most `limit` entries, oldest first.")
  public ReplayResponse replayAll(
      @RequestParam(required = false) String type,
      @RequestParam(required = false) String queue,
      @RequestParam(required = false) String reason,
      @RequestParam(defaultValue = "1000") @Min(1) @Max(10000) int limit) {
    List<UUID> ids = service.replayAll(new DeadLetterFilter(type, queue, reason, null), limit);
    return new ReplayResponse(ids.size(), ids);
  }
}
