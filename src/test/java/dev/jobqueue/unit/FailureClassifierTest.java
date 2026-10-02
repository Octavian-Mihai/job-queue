package dev.jobqueue.unit;

import static org.assertj.core.api.Assertions.assertThat;

import dev.jobqueue.handler.NonRetryableException;
import dev.jobqueue.handler.RetryableException;
import dev.jobqueue.worker.FailureClassifier;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class FailureClassifierTest {

  @Test
  void retryableExceptionIsRetryable() {
    var c = FailureClassifier.classify(new RetryableException("503"));
    assertThat(c.retryable()).isTrue();
    assertThat(c.outcome()).isEqualTo("FAILED_RETRYABLE");
  }

  @Test
  void nonRetryableExceptionIsNotRetryable() {
    var c = FailureClassifier.classify(new NonRetryableException("bad input"));
    assertThat(c.retryable()).isFalse();
    assertThat(c.outcome()).isEqualTo("FAILED_NON_RETRYABLE");
  }

  @Test
  void unknownExceptionsDefaultToRetryable() {
    assertThat(FailureClassifier.classify(new IllegalStateException("bug")).retryable()).isTrue();
    assertThat(FailureClassifier.classify(new IOException("reset")).retryable()).isTrue();
    assertThat(FailureClassifier.classify(new StackOverflowError()).retryable()).isTrue();
  }

  @Test
  void wrappedNonRetryableIsStillNonRetryable() {
    var wrapped = new ExecutionException(new NonRetryableException("nope"));
    assertThat(FailureClassifier.classify(wrapped).retryable()).isFalse();
  }

  @Test
  void nearestClassifiedExceptionWins() {
    var retryableOverNonRetryable =
        new RetryableException("outer says retry", new NonRetryableException("inner"));
    assertThat(FailureClassifier.classify(retryableOverNonRetryable).retryable()).isTrue();

    var nonRetryableOverRetryable =
        new NonRetryableException("outer says stop", new RetryableException("inner"));
    assertThat(FailureClassifier.classify(nonRetryableOverRetryable).retryable()).isFalse();
  }

  @Test
  void cyclicCauseChainsTerminate() {
    var a = new RuntimeException("a");
    var b = new RuntimeException("b", a);
    a.initCause(b);
    assertThat(FailureClassifier.classify(a).retryable()).isTrue();
  }
}
