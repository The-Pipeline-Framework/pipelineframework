package org.pipelineframework.awaitable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Coordinates await interaction persistence, dispatch, completion admission, and query paths.
 */
@ApplicationScoped
public class AwaitCoordinator {

    @Inject
    Instance<AwaitInteractionStore> stores;

    @Inject
    Instance<AwaitTransportAdapter<?, ?>> adapters;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    /**
     * Creates or returns an active interaction for an await step.
     */
    public Uni<AwaitCreateResult> createOrGet(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String assignee,
        String group
    ) {
        long now = System.currentTimeMillis();
        long deadline = now + descriptor.timeout().toMillis();
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String idempotencyKey = deriveIdempotencyKey(descriptor, executionId, requestPayload);
        String correlationId = deriveCorrelationId(descriptor, tenantId, executionId, idempotencyKey);
        return store().createOrGet(new AwaitCreateCommand(
            tenantId,
            executionId,
            descriptor.stepId(),
            stepIndex,
            descriptor.outputType(),
            causationId,
            idempotencyKey,
            correlationId,
            requestPayload,
            assignee,
            group,
            descriptor.transportType(),
            now,
            deadline,
            ttl));
    }

    /**
     * Dispatches an existing interaction through its configured transport adapter.
     */
    @SuppressWarnings("unchecked")
    public Uni<AwaitInteractionRecord> dispatch(AwaitStepDescriptor descriptor, AwaitInteractionRecord interaction) {
        AwaitTransportAdapter<Object, Object> adapter = (AwaitTransportAdapter<Object, Object>) adapter(descriptor.transportType());
        return adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                interaction,
                interaction.requestPayload()))
            .onItem().transformToUni(result -> store().markDispatched(
                interaction.tenantId(),
                interaction.interactionId(),
                interaction.version(),
                result.metadata(),
                System.currentTimeMillis()))
            .onItem().transform(optional -> optional.orElseThrow(() ->
                new IllegalStateException("Await interaction dispatch transition lost OCC race: "
                    + interaction.interactionId())));
    }

    /**
     * Accepts a correlated completion.
     */
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        return store().complete(command);
    }

    /**
     * Queries pending interactions.
     */
    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return store().queryPending(tenantId, assignee, group, stepId, limit);
    }

    /**
     * Finds interactions past their deadline.
     */
    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return store().findTimedOut(nowEpochMs, limit);
    }

    /**
     * Marks an interaction as timed out.
     */
    public Uni<java.util.Optional<AwaitInteractionRecord>> markTimedOut(AwaitInteractionRecord record, long nowEpochMs) {
        return store().markTimedOut(record.tenantId(), record.interactionId(), record.version(), nowEpochMs);
    }

    private AwaitInteractionStore store() {
        String provider = orchestratorConfig == null ? null : orchestratorConfig.stateProvider();
        return stores.stream()
            .filter(candidate -> provider == null || provider.isBlank() || provider.equalsIgnoreCase(candidate.providerName()))
            .sorted((left, right) -> Integer.compare(right.priority(), left.priority()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No AwaitInteractionStore provider is available"
                + (provider == null || provider.isBlank() ? "" : " for provider " + provider)));
    }

    private AwaitTransportAdapter<?, ?> adapter(String type) {
        return adapters.stream()
            .filter(candidate -> type.equalsIgnoreCase(candidate.type()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No AwaitTransportAdapter provider is available for type " + type));
    }

    private String deriveCorrelationId(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String executionId,
        String idempotencyKey) {
        String basis = tenantId + ":" + executionId + ":" + descriptor.stepId() + ":" + idempotencyKey;
        return switch (descriptor.correlationStrategy()) {
            case "interactionId", "signedResumeToken" -> java.util.UUID.nameUUIDFromBytes(
                basis.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
            default -> java.util.UUID.nameUUIDFromBytes(
                (descriptor.correlationStrategy() + ":" + basis).getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
        };
    }

    private String deriveIdempotencyKey(AwaitStepDescriptor descriptor, String executionId, Object requestPayload) {
        if (descriptor.idempotencyKeyFields().isEmpty()) {
            return executionId + ":" + descriptor.stepId();
        }
        JsonNode node = PipelineJson.mapper().valueToTree(requestPayload);
        StringBuilder builder = new StringBuilder(descriptor.stepId());
        for (String field : descriptor.idempotencyKeyFields()) {
            builder.append(':').append(field).append('=');
            JsonNode value = node == null ? null : node.get(field);
            builder.append(value == null || value.isNull() ? "<null>" : value.asText());
        }
        return builder.toString();
    }
}
