package org.pipelineframework;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

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
    if (input instanceof Uni<?> uni) {
      return uni.onItem().transform(item -> new ExecutionInputSnapshot(ExecutionInputShape.UNI, item));
    }
    if (input instanceof Multi<?> multi) {
      return multi.collect().asList().onItem().transform(list ->
          new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.copyOf(list)));
    }
    return Uni.createFrom().item(new ExecutionInputSnapshot(ExecutionInputShape.RAW, input));
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
    if (inputPayload instanceof ExecutionInputSnapshot snapshot) {
      if (snapshot.shape() == ExecutionInputShape.MULTI) {
        Object payload = snapshot.payload();
        if (payload == null) {
          return Multi.createFrom().empty();
        }
        if (payload instanceof Iterable<?> iterable) {
          return Multi.createFrom().iterable(iterable);
        }
        return Multi.createFrom().item(payload);
      }
      return Uni.createFrom().item(snapshot.payload());
    }
    // Backward-compatible replay for records persisted before shape metadata.
    if (inputPayload instanceof List<?> list) {
      return Multi.createFrom().iterable(list);
    }
    return Uni.createFrom().item(inputPayload);
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
