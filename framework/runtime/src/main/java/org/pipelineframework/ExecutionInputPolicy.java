package org.pipelineframework;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Input normalization and idempotency-key policy for queue orchestration paths.
 */
@ApplicationScoped
class ExecutionInputPolicy {

  @Inject
  PipelineOrchestratorConfig orchestratorConfig;

  RuntimeException validateInputShape(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return null;
    }
    return new IllegalArgumentException(MessageFormat.format(
        "Pipeline input must be Uni or Multi, got: {0}",
        input == null ? "null" : input.getClass().getName()));
  }

  Object normalizeExecutionInput(Object input) {
    if (input instanceof Uni<?> || input instanceof Multi<?>) {
      return input;
    }
    return Uni.createFrom().item(input);
  }

  Uni<ExecutionInputSnapshot> resolveExecutionInputPayload(Object input) {
    return resolveExecutionInputPayload(input, PipelineContextHolder.get());
  }

  Uni<ExecutionInputSnapshot> resolveExecutionInputPayload(Object input, PipelineContext pipelineContext) {
    if (input instanceof Uni<?> uni) {
      return uni.onItem().transform(item ->
          new ExecutionInputSnapshot(ExecutionInputShape.UNI, item, pipelineContext));
    }
    if (input instanceof Multi<?> multi) {
      return multi.collect().asList().onItem().transform(list ->
          new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.copyOf(list), pipelineContext));
    }
    return Uni.createFrom().item(new ExecutionInputSnapshot(ExecutionInputShape.RAW, input, pipelineContext));
  }

  String normalizeTenant(String tenantId) {
    if (tenantId == null || tenantId.isBlank()) {
      return orchestratorConfig.defaultTenant();
    }
    return tenantId.trim();
  }

  String resolveExecutionKey(String tenantId, Object input, String clientKey) {
    OrchestratorIdempotencyPolicy policy = orchestratorConfig.idempotencyPolicy();
    String normalizedClientKey = normalizeOptional(clientKey);
    if (policy == OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED) {
      if (normalizedClientKey == null) {
        throw new IllegalArgumentException("Idempotency-Key header is required.");
      }
      return normalizedClientKey;
    }
    if (policy == OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY && normalizedClientKey != null) {
      return normalizedClientKey;
    }
    return deriveServerExecutionKey(tenantId, input);
  }

  Object toReplayInput(Object inputPayload) {
    return rehydrateExecutionInput(inputPayload).reactiveInput();
  }

  RehydratedExecutionInput rehydrateExecutionInput(Object inputPayload) {
    if (inputPayload instanceof ExecutionInputSnapshot snapshot) {
      if (snapshot.shape() == ExecutionInputShape.MULTI) {
        Object payload = snapshot.payload();
        if (payload == null) {
          return new RehydratedExecutionInput(Multi.createFrom().empty(), snapshot.pipelineContext());
        }
        if (payload instanceof Iterable<?> iterable) {
          return new RehydratedExecutionInput(
              Multi.createFrom().iterable(iterable), snapshot.pipelineContext());
        }
        return new RehydratedExecutionInput(Multi.createFrom().item(payload), snapshot.pipelineContext());
      }
      return new RehydratedExecutionInput(
          Uni.createFrom().item(snapshot.payload()), snapshot.pipelineContext());
    }
    // Backward-compatible replay for records persisted before shape metadata.
    if (inputPayload instanceof List<?> list) {
      return new RehydratedExecutionInput(Multi.createFrom().iterable(list), Optional.empty());
    }
    return new RehydratedExecutionInput(Uni.createFrom().item(inputPayload), Optional.empty());
  }

  record RehydratedExecutionInput(Object reactiveInput, Optional<PipelineContext> pipelineContext) {
    RehydratedExecutionInput {
      java.util.Objects.requireNonNull(reactiveInput, "reactiveInput must not be null");
      pipelineContext = Optional.ofNullable(pipelineContext).orElseGet(Optional::empty);
    }
  }

  private String normalizeOptional(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }

  String deriveServerExecutionKey(String tenantId, Object input) {
    try {
      byte[] payloadBytes = org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsBytes(input);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(tenantId.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) ':');
      digest.update(payloadBytes);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to derive deterministic execution key.", e);
    }
  }
}
