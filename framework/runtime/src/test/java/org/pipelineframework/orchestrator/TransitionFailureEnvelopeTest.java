package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandRetryTestAccess;
import org.pipelineframework.step.NonRetryableException;

class TransitionFailureEnvelopeTest {

  @Test
  void preservesFailedStepAcrossWorkerFailureEnvelope() {
    TransitionFailureEnvelope envelope = TransitionFailureRuntimeAdapter.from(
        new IllegalStateException("archive failed"),
        13);

    TransitionWorkerFailureException failure =
        (TransitionWorkerFailureException) TransitionFailureRuntimeAdapter.toException(envelope);

    assertEquals(13, envelope.failedStepIndex());
    assertEquals(IllegalStateException.class.getName(), envelope.failureClass());
    assertEquals("archive failed", envelope.message());
    assertEquals(13, failure.failedStepIndex());
  }

  @Test
  void serializesClassifiedNonRetryableCause() {
    Throwable failure = new IllegalStateException(
        "outer failure",
        new CustomNonRetryableException("do not retry"));

    TransitionFailureEnvelope envelope = TransitionFailureRuntimeAdapter.from(failure, 4);
    RuntimeException decoded = TransitionFailureRuntimeAdapter.toException(envelope);

    assertEquals(CustomNonRetryableException.class.getName(), envelope.failureClass());
    assertEquals("do not retry", envelope.message());
    assertEquals(4, envelope.failedStepIndex());
    assertEquals("do not retry", assertInstanceOf(NonRetryableException.class, decoded).getMessage());
  }

  @Test
  void normalizesMissingThrowableMessage() {
    TransitionFailureEnvelope envelope = TransitionFailureRuntimeAdapter.from(new IllegalStateException(), -1);

    assertEquals("", envelope.message());
  }

  @Test
  void preservesExactRetryableCommandAcrossPortableFailureRoundTrip() {
    Throwable retryable = CommandRetryTestAccess.retryableFailure(
        "archive:confirmation-7", new IllegalStateException("archive failed"));

    TransitionFailureEnvelope envelope = TransitionFailureRuntimeAdapter.from(retryable, 3);
    TransitionWorkerFailureException decoded =
        (TransitionWorkerFailureException) TransitionFailureRuntimeAdapter.toException(envelope);

    assertEquals(3, envelope.failedStepIndex());
    assertEquals(Optional.of("archive:confirmation-7"), envelope.failedCommandId());
    assertEquals(Optional.of("archive:confirmation-7"), decoded.failedCommandId());
  }

  private static final class CustomNonRetryableException extends NonRetryableException {
    private CustomNonRetryableException(String message) {
      super(message);
    }
  }
}
