package org.pipelineframework;

import java.util.Objects;

/**
 * Internal failure marker that retains the pipeline step which produced a transition failure.
 */
final class PipelineStepExecutionFailure extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final int stepIndex;

  private PipelineStepExecutionFailure(int stepIndex, Throwable cause) {
    super(cause.getMessage(), cause);
    this.stepIndex = stepIndex;
  }

  static Throwable at(int stepIndex, Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    return find(failure) != null
        ? failure
        : new PipelineStepExecutionFailure(stepIndex, failure);
  }

  /** Records the resumable root step while discarding any nested definition-local index. */
  static Throwable atRoot(int stepIndex, Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    return new PipelineStepExecutionFailure(stepIndex, source(failure));
  }

  static int stepIndex(Throwable failure) {
    PipelineStepExecutionFailure indexed = find(failure);
    return indexed == null ? -1 : indexed.stepIndex;
  }

  static Throwable source(Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    PipelineStepExecutionFailure indexed = find(failure);
    return indexed != null && indexed.getCause() != null
        ? indexed.getCause()
        : failure;
  }

  private static PipelineStepExecutionFailure find(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current.getClass() == PipelineStepExecutionFailure.class) {
        return (PipelineStepExecutionFailure) current;
      }
      Throwable cause = current.getCause();
      current = cause == current ? null : cause;
    }
    return null;
  }
}
