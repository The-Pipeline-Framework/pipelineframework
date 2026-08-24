package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;

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
    return find(failure).isPresent()
        ? failure
        : new PipelineStepExecutionFailure(stepIndex, failure);
  }

  /** Records the resumable root step while discarding any nested definition-local index. */
  static Throwable atRoot(int stepIndex, Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    return new PipelineStepExecutionFailure(stepIndex, source(failure));
  }

  static int stepIndex(Throwable failure) {
    return find(failure)
        .map(indexed -> indexed.stepIndex)
        .orElse(-1);
  }

  static Throwable source(Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    return find(failure)
        .map(Throwable::getCause)
        .orElse(failure);
  }

  private static Optional<PipelineStepExecutionFailure> find(Throwable failure) {
    Objects.requireNonNull(failure, "failure");
    Throwable current = failure;
    while (current != null) {
      if (current.getClass() == PipelineStepExecutionFailure.class) {
        return Optional.of((PipelineStepExecutionFailure) current);
      }
      Throwable cause = current.getCause();
      current = cause == current ? null : cause;
    }
    return Optional.empty();
  }
}
