package dev.jobqueue.worker;

import dev.jobqueue.config.ConditionalOnRole;
import dev.jobqueue.config.Role;
import dev.jobqueue.metrics.JobMetrics;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Periodically reclaims jobs whose worker stopped heartbeating. Runs on every worker: the SQL is
 * safe under concurrency, so there is no leader election and no single point of failure; any
 * surviving worker recovers a crashed worker's jobs.
 */
@Component
@ConditionalOnRole(Role.WORKER)
public class Reaper implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(Reaper.class);

  private final LeaseRepository leases;
  private final WorkerProperties props;
  private final JobMetrics metrics;
  private ScheduledExecutorService scheduler;
  private volatile boolean running;

  public Reaper(LeaseRepository leases, WorkerProperties props, JobMetrics metrics) {
    this.leases = leases;
    this.props = props;
    this.metrics = metrics;
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    running = true;
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "job-reaper");
              t.setDaemon(true);
              return t;
            });
    long ms = props.reaperInterval().toMillis();
    scheduler.scheduleWithFixedDelay(this::reapSafely, ms, ms, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void stop() {
    running = false;
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private void reapSafely() {
    try {
      reapAll();
    } catch (RuntimeException e) {
      log.warn("reaper pass failed, will retry: {}", e.toString());
    }
  }

  /** Drains all currently-expired leases (in batches); package-visible for tests. */
  int reapAll() {
    int total = 0;
    List<LeaseRepository.Reaped> batch;
    do {
      batch = leases.reapExpired(props.reaperBatchSize());
      total += batch.size();
      for (LeaseRepository.Reaped r : batch) {
        boolean dead = r.newStatus().equals("DEAD");
        metrics.leaseReclaimed(r.type(), dead ? "dead" : "requeued");
        if (dead) {
          metrics.dead(r.type(), DeadReason.MAX_ATTEMPTS_EXCEEDED);
        }
      }
      if (!batch.isEmpty()) {
        long dead = batch.stream().filter(r -> r.newStatus().equals("DEAD")).count();
        log.warn(
            "reaper reclaimed {} job(s) with expired leases ({} requeued, {} dead-lettered)",
            batch.size(),
            batch.size() - dead,
            dead);
      }
    } while (batch.size() == props.reaperBatchSize());
    return total;
  }
}
