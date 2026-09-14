package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandRetryTestAccess;

class TransitionFailureEnvelopeTest {

  @Test
  void preservesFailedStepAcrossWorkerFailureEnvelope() {
    TransitionFailureEnvelope envelope = TransitionFailureRuntimeAdapter.from(
        new IllegalStateException("archive failed"),
        13);

    TransitionWorkerFailureException failure =
        (TransitionWorkerFailureException) TransitionFailureRuntimeAdapter.toException(envelope);

    assertEquals(13, envelope.failedStepIndex());
    assertEquals(13, failure.failedStepIndex());
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
}
