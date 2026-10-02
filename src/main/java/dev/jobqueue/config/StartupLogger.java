package dev.jobqueue.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
class StartupLogger {

  private static final Logger log = LoggerFactory.getLogger(StartupLogger.class);

  private final JobQueueProperties props;

  StartupLogger(JobQueueProperties props) {
    this.props = props;
  }

  @EventListener(ApplicationReadyEvent.class)
  void onReady() {
    log.info("job-queue started roles={} workerId={}", props.roles(), props.workerId());
  }
}
