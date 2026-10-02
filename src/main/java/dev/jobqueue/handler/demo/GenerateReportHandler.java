package dev.jobqueue.handler.demo;

import dev.jobqueue.handler.JobContext;
import dev.jobqueue.handler.JobHandler;
import org.springframework.stereotype.Component;

/**
 * Simulated slow job: burns CPU for {@code cpuMs}, then waits {@code sleepMs} (e.g. a slow query).
 * Payload: {@code {"cpuMs": 200, "sleepMs": 2000}}; both optional and capped. Responds to
 * interruption so a timeout or shutdown can stop it.
 */
@Component
public class GenerateReportHandler implements JobHandler {

  @Override
  public String type() {
    return "generate-report";
  }

  @Override
  public void handle(JobContext ctx) throws Exception {
    long cpuMs = Math.min(ctx.payload().path("cpuMs").asLong(0), 10_000);
    long sleepMs = Math.min(ctx.payload().path("sleepMs").asLong(2_000), 60_000);

    long deadline = System.nanoTime() + cpuMs * 1_000_000;
    double sink = 0;
    while (System.nanoTime() < deadline) {
      sink += Math.sqrt(sink + 1.0001);
      if (Thread.interrupted()) {
        throw new InterruptedException("report generation interrupted");
      }
    }
    if (sink < 0) { // never true; keeps the loop from being optimised away
      throw new IllegalStateException();
    }
    Thread.sleep(sleepMs);
  }
}
