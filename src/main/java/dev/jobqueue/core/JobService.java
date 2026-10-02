package dev.jobqueue.core;

import dev.jobqueue.handler.HandlerRegistry;
import dev.jobqueue.metrics.JobMetrics;
import dev.jobqueue.retry.RetryPolicies;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class JobService {

  private final JobRepository repo;
  private final HandlerRegistry registry;
  private final RetryPolicies policies;
  private final JobMetrics metrics;

  public JobService(
      JobRepository repo, HandlerRegistry registry, RetryPolicies policies, JobMetrics metrics) {
    this.repo = repo;
    this.registry = registry;
    this.policies = policies;
    this.metrics = metrics;
  }

  public EnqueueResult enqueue(NewJob newJob) {
    // Reject typos up front instead of letting the job fail later with "no handler".
    if (!registry.isKnown(newJob.type())) {
      throw new UnknownJobTypeException(newJob.type(), registry.types());
    }
    if (newJob.maxAttempts() == null) {
      newJob =
          new NewJob(
              newJob.queueName(),
              newJob.type(),
              newJob.payload(),
              newJob.priority(),
              policies.resolve(newJob.type()).maxAttempts(),
              newJob.runAt(),
              newJob.delaySeconds(),
              newJob.idempotencyKey());
    }
    var id = repo.insertIfAbsent(newJob);
    if (id.isPresent()) {
      metrics.enqueued(newJob.type(), true);
      return new EnqueueResult(repo.findById(id.get()).orElseThrow(), true);
    }
    // Conflict on the idempotency key: the winning row is committed by now, so this read sees it.
    Job existing = repo.findByIdempotencyKey(newJob.idempotencyKey()).orElseThrow();
    metrics.enqueued(newJob.type(), false);
    return new EnqueueResult(existing, false);
  }

  public Job get(UUID id) {
    return repo.findById(id).orElseThrow(() -> new JobNotFoundException(id));
  }

  public List<JobAttempt> attempts(UUID id) {
    return repo.findAttempts(id);
  }

  public PageResult<Job> list(JobStatus status, String type, String queue, int page, int size) {
    return repo.list(status, type, queue, page, size);
  }

  /** Atomic conditional update: a worker claiming the job concurrently wins or loses cleanly. */
  public Job cancel(UUID id) {
    if (repo.cancelIfPending(id)) {
      return get(id);
    }
    Job job = get(id); // throws 404 if absent
    throw new JobStateConflictException(
        "Job " + id + " is " + job.status() + "; only PENDING jobs can be cancelled");
  }
}
