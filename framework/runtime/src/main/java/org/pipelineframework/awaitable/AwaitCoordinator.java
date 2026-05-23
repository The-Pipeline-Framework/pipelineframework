package org.pipelineframework.awaitable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
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
    Instance<AwaitTransportAdapter<?>> adapters;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    AwaitResumeTokenService resumeTokenService;

    private volatile AwaitInteractionStore resolvedStore;
    private final Map<String, AwaitTransportAdapter<?>> resolvedAdapters = new ConcurrentHashMap<>();

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
        Object normalizedRequestPayload = AwaitPayloadSupport.normalize(requestPayload);
        long now = System.currentTimeMillis();
        long deadline = now + descriptor.timeout().toMillis();
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String idempotencyKey = deriveIdempotencyKey(descriptor, executionId, normalizedRequestPayload);
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
            normalizedRequestPayload,
            assignee,
            group,
            descriptor.transportType(),
            now,
            deadline,
            ttl));
    }

    /**
     * Creates or returns one item interaction inside a per-item await barrier.
     */
    public Uni<AwaitCreateResult> createOrGetBarrierItem(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String barrierId,
        int itemIndex,
        int itemCount,
        String assignee,
        String group
    ) {
        Object normalizedRequestPayload = AwaitPayloadSupport.normalize(requestPayload);
        if (itemCount <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("itemCount must be greater than 0"));
        }
        if (itemIndex < 0 || itemIndex >= itemCount) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "itemIndex must be in range [0, itemCount), got " + itemIndex + " for itemCount=" + itemCount));
        }
        long now = System.currentTimeMillis();
        long deadline = now + descriptor.timeout().toMillis();
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String idempotencyKey = deriveIdempotencyKey(descriptor, executionId, normalizedRequestPayload) + ":item=" + itemIndex;
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
            normalizedRequestPayload,
            assignee,
            group,
            descriptor.transportType(),
            barrierId,
            itemIndex,
            itemCount,
            now,
            deadline,
            ttl));
    }

    /**
     * Dispatches an existing interaction through its configured transport adapter.
     */
    @SuppressWarnings("unchecked")
    public Uni<AwaitInteractionRecord> dispatch(AwaitStepDescriptor descriptor, AwaitInteractionRecord interaction) {
        AwaitTransportAdapter<Object> adapter = (AwaitTransportAdapter<Object>) adapter(descriptor.transportType());
        long nowEpochMs = System.currentTimeMillis();
        return store().markDispatching(
                interaction.tenantId(),
                interaction.interactionId(),
                interaction.version(),
                nowEpochMs)
            .onItem().transform(optional -> optional.orElseThrow(() ->
                new IllegalStateException("Await interaction dispatch transition lost OCC race: "
                    + interaction.interactionId())))
            .onItem().transformToUni(claimedInteraction -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                    descriptor,
                    claimedInteraction,
                    claimedInteraction.requestPayload()))
                .onFailure().call(failure -> store().fail(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    failure.getMessage(),
                    System.currentTimeMillis())
                    .replaceWith(failure))
                .onItem().transformToUni(result -> store().markDispatched(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    result.metadata(),
                    System.currentTimeMillis()))
                .onItem().transform(optional -> optional.orElseThrow(() ->
                    new IllegalStateException("Await interaction metadata update lost OCC race: "
                        + claimedInteraction.interactionId()))));
    }

    /**
     * Accepts a correlated completion.
     */
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        AwaitCompletionCommand normalized = normalizeCompletionCommand(command);
        if (normalized.resumeToken() == null) {
            return store().complete(normalized);
        }
        return resolveForCompletion(normalized)
            .onItem().transformToUni(record -> {
                if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
                    return Uni.createFrom().failure(
                        new AwaitInteractionTerminalException("Await interaction is terminal: " + record.status()));
                }
                return Uni.createFrom().item(() -> {
                        resumeTokenService.validate(normalized.resumeToken(), record, normalized.nowEpochMs());
                        return record;
                    })
                    .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                    .replaceWith(record);
            })
            .onItem().transformToUni(ignored -> store().complete(normalized));
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
     * Returns all interactions for a per-item await barrier.
     */
    public Uni<List<AwaitInteractionRecord>> findByBarrier(
        String tenantId,
        String executionId,
        int stepIndex,
        String barrierId) {
        return store().findByBarrier(tenantId, executionId, stepIndex, barrierId);
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
        AwaitInteractionStore cached = resolvedStore;
        if (cached != null) {
            return cached;
        }
        String provider = orchestratorConfig == null ? null : orchestratorConfig.stateProvider();
        synchronized (this) {
            if (resolvedStore == null) {
                resolvedStore = stores.stream()
                    .filter(candidate -> provider == null || provider.isBlank() || provider.equalsIgnoreCase(candidate.providerName()))
                    .sorted((left, right) -> {
                        int priorityComparison = Integer.compare(right.priority(), left.priority());
                        if (priorityComparison != 0) {
                            return priorityComparison;
                        }
                        return left.providerName().compareToIgnoreCase(right.providerName());
                    })
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No AwaitInteractionStore provider is available"
                        + (provider == null || provider.isBlank() ? "" : " for provider " + provider)));
            }
            return resolvedStore;
        }
    }

    private AwaitTransportAdapter<?> adapter(String type) {
        return resolvedAdapters.computeIfAbsent(type.toLowerCase(java.util.Locale.ROOT), ignored -> resolveAdapter(type));
    }

    private AwaitTransportAdapter<?> resolveAdapter(String type) {
        List<AwaitTransportAdapter<?>> matching = adapters.stream()
            .filter(candidate -> type.equalsIgnoreCase(candidate.type()))
            .toList();
        if (matching.isEmpty()) {
            throw new IllegalStateException("No AwaitTransportAdapter provider is available for type " + type);
        }
        if (matching.size() > 1) {
            String providerInfo = matching.stream()
                .map(candidate -> candidate.getClass().getName())
                .collect(java.util.stream.Collectors.joining(", "));
            throw new IllegalStateException("Ambiguous AwaitTransportAdapter providers for type " + type + ": " + providerInfo);
        }
        return matching.get(0);
    }

    private Uni<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        Uni<java.util.Optional<AwaitInteractionRecord>> lookup = command.interactionId() != null
            ? store().get(command.tenantId(), command.interactionId())
            : store().findByCorrelation(command.tenantId(), command.correlationId());
        return lookup.onItem().transform(optional -> optional.orElseThrow(
            () -> new AwaitInteractionNotFoundException("No await interaction matches completion")));
    }

    private static AwaitCompletionCommand normalizeCompletionCommand(AwaitCompletionCommand command) {
        return new AwaitCompletionCommand(
            command.tenantId(),
            command.interactionId(),
            command.correlationId(),
            command.resumeToken(),
            command.idempotencyKey(),
            AwaitPayloadSupport.normalize(command.responsePayload()),
            command.actor(),
            command.nowEpochMs());
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
