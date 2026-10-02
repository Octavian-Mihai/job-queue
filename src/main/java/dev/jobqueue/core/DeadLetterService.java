package dev.jobqueue.core;

import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterService {

  private final DeadLetterRepository repo;
  private final JobRepository jobs;

  public DeadLetterService(DeadLetterRepository repo, JobRepository jobs) {
    this.repo = repo;
    this.jobs = jobs;
  }

  public PageResult<DeadLetter> list(DeadLetterFilter filter, int page, int size) {
    return repo.list(filter, page, size);
  }

  public DeadLetter get(UUID id) {
    return repo.findById(id).orElseThrow(() -> new DeadLetterNotFoundException(id));
  }

  /** The dead job's full attempt history (kept on the original job row). */
  public List<JobAttempt> attempts(DeadLetter entry) {
    return jobs.findAttempts(entry.jobId());
  }

  public UUID replayOne(UUID id) {
    List<UUID> created = repo.replay(id, null, 1);
    if (!created.isEmpty()) {
      return created.get(0);
    }
    get(id); // 404 if it does not exist
    throw new JobStateConflictException(
        "Dead letter " + id + " was already replayed (or is being replayed right now)");
  }

  public List<UUID> replayAll(DeadLetterFilter filter, int limit) {
    return repo.replay(null, filter, limit);
  }
}
