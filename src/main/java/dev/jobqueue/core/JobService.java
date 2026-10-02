package dev.jobqueue.core;

import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class JobService {

  private final JobRepository repo;

  public JobService(JobRepository repo) {
    this.repo = repo;
  }

  public EnqueueResult enqueue(NewJob newJob) {
    var id = repo.insertIfAbsent(newJob);
    if (id.isPresent()) {
      return new EnqueueResult(repo.findById(id.get()).orElseThrow(), true);
    }
    // Conflict on the idempotency key: the winning row is committed by now, so this read sees it.
    Job existing = repo.findByIdempotencyKey(newJob.idempotencyKey()).orElseThrow();
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
