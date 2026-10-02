package dev.jobqueue.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.jobqueue.config.ConditionalOnRole;
import dev.jobqueue.config.Role;
import dev.jobqueue.core.EnqueueResult;
import dev.jobqueue.core.Job;
import dev.jobqueue.core.JobService;
import dev.jobqueue.core.JobStatus;
import dev.jobqueue.core.NewJob;
import dev.jobqueue.core.PageResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
@ConditionalOnRole(Role.API)
@Tag(name = "Jobs")
public class JobController {

  private final JobService service;

  public JobController(JobService service) {
    this.service = service;
  }

  @PostMapping
  @Operation(
      summary = "Enqueue a job",
      description =
          "Returns 201 with the new job. If Idempotency-Key matches an existing job, returns 200"
              + " with that job instead (no duplicate is created).")
  public ResponseEntity<JobResponse> create(
      @Valid @RequestBody CreateJobRequest req,
      @RequestHeader(value = "Idempotency-Key", required = false) @Size(min = 1, max = 255)
          String idempotencyKey) {
    JsonNode payload =
        req.payload() == null || req.payload().isNull()
            ? JsonNodeFactory.instance.objectNode()
            : req.payload();
    NewJob newJob =
        new NewJob(
            req.queue() == null ? "default" : req.queue(),
            req.type(),
            payload,
            req.priority() == null ? 0 : req.priority(),
            req.maxAttempts(),
            req.runAt(),
            req.delaySeconds() == null ? 0 : req.delaySeconds(),
            idempotencyKey);
    EnqueueResult result = service.enqueue(newJob);
    JobResponse body = new JobResponse(result.job(), service.attempts(result.job().id()));
    URI location = URI.create("/jobs/" + result.job().id());
    return result.created()
        ? ResponseEntity.created(location).body(body)
        : ResponseEntity.ok().location(location).body(body);
  }

  @GetMapping("/{id}")
  @Operation(summary = "Get a job with its attempt history")
  public JobResponse get(@PathVariable UUID id) {
    return new JobResponse(service.get(id), service.attempts(id));
  }

  @GetMapping
  @Operation(summary = "List jobs, newest first")
  public PageResult<Job> list(
      @RequestParam(required = false) JobStatus status,
      @RequestParam(required = false) String type,
      @RequestParam(required = false) String queue,
      @RequestParam(defaultValue = "0") @Min(0) int page,
      @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
    return service.list(status, type, queue, page, size);
  }

  @PostMapping("/{id}/cancel")
  @Operation(summary = "Cancel a job (only while PENDING)")
  public JobResponse cancel(@PathVariable UUID id) {
    return new JobResponse(service.cancel(id), service.attempts(id));
  }
}
