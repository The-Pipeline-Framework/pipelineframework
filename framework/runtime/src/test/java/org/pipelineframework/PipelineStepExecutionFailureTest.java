package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineStepExecutionFailureTest {

  @Test
  void rejectsMissingFailuresAtEveryEntryPoint() {
    assertThrows(NullPointerException.class, () -> PipelineStepExecutionFailure.at(1, null));
    assertThrows(NullPointerException.class, () -> PipelineStepExecutionFailure.atRoot(1, null));
    assertThrows(NullPointerException.class, () -> PipelineStepExecutionFailure.source(null));
  }
}
