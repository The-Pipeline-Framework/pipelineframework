package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

record RunAcceptance(CreateExecutionResult created, long acceptedAtEpochMs) {

  RunAcceptance {
    Objects.requireNonNull(created, "created must not be null");
  }

  RunAsyncAcceptedDto toDto() {
    String executionId = created.record().executionId();
    return new RunAsyncAcceptedDto(
        executionId,
        created.duplicate(),
        "/pipeline/executions/" + executionId,
        acceptedAtEpochMs);
  }
}
