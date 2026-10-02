package dev.jobqueue.worker;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Worker tuning ({@code JOBQUEUE_WORKER_*} env vars).
 *
 * @param concurrency max jobs executing at once in this process
 * @param batchSize max jobs claimed per poll (never more than the free slots)
 * @param pollInterval wait between polls while busy; doubles up to {@code maxPollInterval} when
 *     idle
 * @param leaseDuration how long a claim is valid without a heartbeat
 * @param heartbeatInterval how often in-flight leases are extended; default leaseDuration / 3
 * @param reaperInterval how often expired leases are reclaimed
 * @param reaperBatchSize max jobs reclaimed per statement
 * @param shutdownGracePeriod how long stop() waits for in-flight jobs before releasing them
 * @param queues queues to consume; empty means all queues
 */
@ConfigurationProperties("jobqueue.worker")
public record WorkerProperties(
    int concurrency,
    int batchSize,
    Duration pollInterval,
    Duration maxPollInterval,
    Duration leaseDuration,
    Duration heartbeatInterval,
    Duration reaperInterval,
    int reaperBatchSize,
    Duration shutdownGracePeriod,
    List<String> queues) {

  public WorkerProperties {
    concurrency = concurrency > 0 ? concurrency : 8;
    batchSize = batchSize > 0 ? batchSize : 10;
    pollInterval = pollInterval != null ? pollInterval : Duration.ofMillis(200);
    maxPollInterval = maxPollInterval != null ? maxPollInterval : Duration.ofSeconds(5);
    leaseDuration = leaseDuration != null ? leaseDuration : Duration.ofSeconds(30);
    heartbeatInterval = heartbeatInterval != null ? heartbeatInterval : leaseDuration.dividedBy(3);
    reaperInterval = reaperInterval != null ? reaperInterval : Duration.ofSeconds(5);
    reaperBatchSize = reaperBatchSize > 0 ? reaperBatchSize : 100;
    shutdownGracePeriod =
        shutdownGracePeriod != null ? shutdownGracePeriod : Duration.ofSeconds(30);
    queues = queues == null ? List.of() : List.copyOf(queues);
    if (heartbeatInterval.compareTo(leaseDuration) >= 0) {
      throw new IllegalArgumentException("heartbeat-interval must be shorter than lease-duration");
    }
    if (maxPollInterval.compareTo(pollInterval) < 0) {
      throw new IllegalArgumentException("max-poll-interval must be >= poll-interval");
    }
  }
}
