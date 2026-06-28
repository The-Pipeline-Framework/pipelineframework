package org.pipelineframework;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;

record ExecutionResultView(ExecutionRecord<Object, Object> record) {

  ExecutionResultView {
    if (record == null) {
      throw new IllegalArgumentException("ExecutionResultView requires record");
    }
  }

  ExecutionStatusDto statusDto() {
    return new ExecutionStatusDto(
        record.executionId(),
        record.status(),
        record.currentStepIndex(),
        record.attempt(),
        record.version(),
        record.nextDueEpochMs(),
        record.updatedAtEpochMs(),
        record.errorCode(),
        record.errorMessage());
  }

  Object typedResult(Class<?> outputType, boolean outputStreaming, Supplier<TransitionPayloadCodec> payloadCodec) {
    List<?> items = successfulItems();
    if (record.resultShape() == ExecutionResultShape.SINGLE) {
      validateSingleShape(items);
      if (outputStreaming) {
        return List.copyOf(items);
      }
      return items.isEmpty() ? null : coerceStoredResult(items.getFirst(), outputType, payloadCodec);
    }
    if (!outputStreaming) {
      throw new IllegalStateException(
          "Execution " + record.executionId()
              + " produced a materialized multi result. Request list retrieval instead.");
    }
    return coerceStoredResults(items, outputType, payloadCodec);
  }

  Object rawPayload() {
    List<?> items = successfulItems();
    if (record.resultShape() == ExecutionResultShape.SINGLE) {
      validateSingleShape(items);
      return items.isEmpty() ? null : items.getFirst();
    }
    return List.copyOf(items);
  }

  Optional<Object> typedOptionalResult(
      Class<?> outputType,
      boolean outputStreaming,
      Supplier<TransitionPayloadCodec> payloadCodec) {
    return Optional.ofNullable(typedResult(outputType, outputStreaming, payloadCodec));
  }

  private List<?> successfulItems() {
    if (record.status() == ExecutionStatus.SUCCEEDED) {
      if (record.resultPayload() == null) {
        return List.of();
      }
      return (List<?>) record.resultPayload();
    }
    if (record.status().terminal()) {
      throw new IllegalStateException("Execution finished without a successful result: " + record.status());
    }
    throw new IllegalStateException("Execution is not complete yet: " + record.status());
  }

  private void validateSingleShape(List<?> items) {
    if (items.size() > 1) {
      throw new IllegalStateException(
          "Execution " + record.executionId() + " stored multiple terminal items for SINGLE result shape");
    }
  }

  private Object coerceStoredResult(
      Object result,
      Class<?> outputType,
      Supplier<TransitionPayloadCodec> payloadCodec) {
    if (result instanceof SerializedTransitionPayload serialized) {
      return coerceStoredResult(payloadCodec.get().decode(serialized), outputType, payloadCodec);
    }
    if (result == null || outputType == null || outputType.isInstance(result)) {
      return result;
    }
    try {
      return PipelineJson.mapper().convertValue(result, outputType);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Failed to coerce stored result from "
              + result.getClass().getName()
              + " to "
              + outputType.getName(),
          e);
    }
  }

  private List<?> coerceStoredResults(
      List<?> results,
      Class<?> outputType,
      Supplier<TransitionPayloadCodec> payloadCodec) {
    if (outputType == null) {
      return List.copyOf(results);
    }
    return results.stream()
        .map(result -> coerceStoredResult(result, outputType, payloadCodec))
        .toList();
  }
}
