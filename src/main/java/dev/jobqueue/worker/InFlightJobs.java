package dev.jobqueue.worker;

import dev.jobqueue.core.Job;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

/** Jobs currently executing in this process: what the heartbeat extends and shutdown releases. */
@Component
public class InFlightJobs {

  /** One running attempt. Flags are set by the heartbeat, timeout and shutdown paths. */
  public static final class Handle {
    private final Lease lease;
    private final Thread thread;
    private final AtomicBoolean timedOut = new AtomicBoolean();
    private volatile boolean leaseLost;
    private volatile boolean released;
    private volatile boolean finishing;

    Handle(Lease lease, Thread thread) {
      this.lease = lease;
      this.thread = thread;
    }

    public Lease lease() {
      return lease;
    }

    public Thread thread() {
      return thread;
    }

    /** Set when the execution timeout fires; the heartbeat then stops renewing this lease. */
    public AtomicBoolean timedOut() {
      return timedOut;
    }

    public boolean leaseLost() {
      return leaseLost;
    }

    public boolean released() {
      return released;
    }

    /**
     * True once the handler has returned and the executor is recording the outcome. From then on
     * the job legitimately stops being RUNNING, so the heartbeat must not read that as a lost
     * lease.
     */
    public boolean finishing() {
      return finishing;
    }

    /** Called by the executor BEFORE it writes the outcome (the write is what ends RUNNING). */
    public void markFinishing() {
      finishing = true;
    }

    void markLeaseLost() {
      leaseLost = true;
    }

    void markReleased() {
      released = true;
    }
  }

  // Keyed by the claim (job + attempt), not the job: a reclaimed job can start its next attempt on
  // this very worker while the zombie previous attempt is still unwinding, and both must be
  // tracked.
  private final Map<Lease, Handle> byClaim = new ConcurrentHashMap<>();

  Handle register(Job job, Thread thread) {
    Handle handle = new Handle(new Lease(job.id(), job.attempts()), thread);
    byClaim.put(handle.lease(), handle);
    return handle;
  }

  void unregister(Handle handle) {
    byClaim.remove(handle.lease(), handle);
  }

  public List<Handle> snapshot() {
    return List.copyOf(byClaim.values());
  }

  public int count() {
    return byClaim.size();
  }
}
