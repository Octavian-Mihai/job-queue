package dev.jobqueue.api;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import dev.jobqueue.core.Job;
import dev.jobqueue.core.JobAttempt;
import java.util.List;

/** A job's fields plus its attempt history. */
public record JobResponse(@JsonUnwrapped Job job, List<JobAttempt> attempts) {}
