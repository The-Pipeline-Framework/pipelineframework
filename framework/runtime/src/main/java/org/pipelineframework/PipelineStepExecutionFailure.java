package org.pipelineframework;

/**
 * Internal failure marker that retains the pipeline step which produced a transition failure.
 */
final class PipelineStepExecutionFailure extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final int stepIndex;

  private PipelineStepExecutionFailure(int stepIndex, Throwable cause) {
    super(cause == null ? null : cause.getMessage(), cause);
    this.stepIndex = stepIndex;
  }

  static Throwable at(int stepIndex, Throwable failure) {
    return find(failure) != null
        ? failure
        : new PipelineStepExecutionFailure(stepIndex, failure);
  }

  static int stepIndex(Throwable failure) {
    PipelineStepExecutionFailure indexed = find(failure);
    return indexed == null ? -1 : indexed.stepIndex;
  }

  static Throwable source(Throwable failure) {
    PipelineStepExecutionFailure indexed = find(failure);
    return indexed != null && indexed.getCause() != null
        ? indexed.getCause()
        : failure;
  }

  private static PipelineStepExecutionFailure find(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof PipelineStepExecutionFailure indexed) {
        return indexed;
      }
      Throwable cause = current.getCause();
      current = cause == current ? null : cause;
    }
    return null;
  }
}
