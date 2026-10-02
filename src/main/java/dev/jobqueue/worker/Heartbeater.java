package dev.jobqueue.worker;

import dev.jobqueue.metrics.JobMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the leases of this worker's in-flight jobs alive with ONE batched UPDATE per interval
 * (rather than a timer and a DB round trip per job).
 *
 * <p>It deliberately skips attempts whose execution timeout fired: a handler that ignores
 * interruption must not hold its job forever, so its lease is left to expire and the reaper returns
 * the job to the queue. And if a heartbeat discovers a lease was lost (the job was reclaimed), the
 * zombie handler is interrupted to stop wasting work; its result would be fenced out anyway.
 */
class Heartbeater {

  private static final Logger log = LoggerFactory.getLogger(Heartbeater.class);

  private final LeaseRepository leases;
  private final InFlightJobs inFlight;
  private final JobMetrics metrics;
  private final String workerId;
  private final Duration leaseDuration;
  private final Duration interval;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "job-heartbeat");
            t.setDaemon(true);
            return t;
          });

  Heartbeater(
      LeaseRepository leases,
      InFlightJobs inFlight,
      JobMetrics metrics,
      String workerId,
      Duration leaseDuration,
      Duration interval) {
    this.leases = leases;
    this.inFlight = inFlight;
    this.metrics = metrics;
    this.workerId = workerId;
    this.leaseDuration = leaseDuration;
    this.interval = interval;
  }

  void start() {
    scheduler.scheduleWithFixedDelay(
        this::beatSafely, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  void stop() {
    scheduler.shutdownNow();
  }

  private void beatSafely() {
    try {
      beat();
    } catch (RuntimeException e) {
      // A scheduled task that throws is silently cancelled; a DB blip must not end heartbeating.
      log.warn("heartbeat failed, will retry next interval: {}", e.toString());
    }
  }

  /** One heartbeat round; package-visible for tests. */
  void beat() {
    List<InFlightJobs.Handle> targets =
        inFlight.snapshot().stream()
            .filter(h -> !h.timedOut().get() && !h.released() && !h.finishing())
            .toList();
    if (targets.isEmpty()) {
      return;
    }
    Set<UUID> extended =
        leases.extend(
            workerId, targets.stream().map(InFlightJobs.Handle::lease).toList(), leaseDuration);
    for (InFlightJobs.Handle h : targets) {
      // Re-read `finishing` AFTER the UPDATE: the executor sets it before writing the outcome, so
      // if the job left RUNNING because it just completed, the flag is already visible here. Only
      // a claim that vanished while its handler was still running is a genuinely lost lease.
      if (!extended.contains(h.lease().jobId()) && !h.finishing()) {
        log.warn(
            "lost lease on job {} attempt {}: interrupting its handler",
            h.lease().jobId(),
            h.lease().attempt());
        metrics.heartbeatLost();
        h.markLeaseLost();
        h.thread().interrupt();
      }
    }
  }
}
