package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TransitionFailureEnvelopeTest {

  @Test
  void preservesFailedStepAcrossWorkerFailureEnvelope() {
    TransitionFailureEnvelope envelope = TransitionFailureEnvelope.from(
        new IllegalStateException("archive failed"),
        13);

    TransitionWorkerFailureException failure =
        (TransitionWorkerFailureException) envelope.toException();

    assertEquals(13, envelope.failedStepIndex());
    assertEquals(13, failure.failedStepIndex());
  }

  @Test
  void normalizesMissingThrowableMessage() {
    TransitionFailureEnvelope envelope = TransitionFailureEnvelope.from(new IllegalStateException());

    assertEquals("", envelope.message());
  }
}
