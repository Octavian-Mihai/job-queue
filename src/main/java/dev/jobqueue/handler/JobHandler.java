package dev.jobqueue.handler;

/**
 * Executes one job type. Register an implementation as a Spring bean and it is picked up by the
 * {@link HandlerRegistry}.
 *
 * <p>Throw {@link RetryableException} / {@link NonRetryableException} to classify a failure; any
 * other exception is treated as retryable. Handlers must be idempotent (see {@code
 * SendEmailHandler}).
 */
public interface JobHandler {

  /** The job type this handler serves; unique across handlers. */
  String type();

  void handle(JobContext context) throws Exception;
}
