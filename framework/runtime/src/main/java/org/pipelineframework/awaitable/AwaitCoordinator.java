package org.pipelineframework.awaitable;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/**
 * Coordinates await unit persistence, interaction dispatch, completion admission, and replay payload loading.
 */
@ApplicationScoped
public class AwaitCoordinator {

    @Inject
    Instance<AwaitInteractionStore> interactionStores;

    @Inject
    Instance<AwaitUnitStore> unitStores;

    @Inject
    Instance<AwaitTransportAdapter<?>> adapters;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    AwaitResumeTokenService resumeTokenService;

    private volatile AwaitInteractionStore resolvedInteractionStore;
    private volatile AwaitUnitStore resolvedUnitStore;
    private final Map<String, AwaitTransportAdapter<?>> resolvedAdapters = new ConcurrentHashMap<>();

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
        String unitId = deriveUnitId(tenantId, executionId, descriptor.stepId(), stepIndex);
        return createOrGetUnit(descriptor, tenantId, unitId, executionId, stepIndex)
            .onItem().transformToUni(unit -> createInteraction(
                descriptor,
                unit.unitId(),
                tenantId,
                executionId,
                stepIndex,
                causationId,
                requestPayload,
                null,
                assignee,
                group)
                .onItem().transformToUni(created -> unitStore().attachPrimaryInteraction(
                        tenantId,
                        unit.unitId(),
                        created.record().interactionId(),
                        System.currentTimeMillis())
                    .replaceWith(created)));
    }

    public Uni<AwaitCreateResult> createOrGetItem(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        String unitId,
        int itemIndex,
        String assignee,
        String group
    ) {
        if (itemIndex < 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("itemIndex must be non-negative"));
        }
        return createOrGetUnit(descriptor, tenantId, unitId, executionId, stepIndex)
            .onItem().transformToUni(ignored -> createInteraction(
                descriptor,
                unitId,
                tenantId,
                executionId,
                stepIndex,
                causationId,
                requestPayload,
                itemIndex,
                assignee,
                group));
    }

    @SuppressWarnings("unchecked")
    public Uni<AwaitInteractionRecord> dispatch(AwaitStepDescriptor descriptor, AwaitInteractionRecord interaction) {
        AwaitTransportAdapter<Object> adapter = (AwaitTransportAdapter<Object>) adapter(descriptor.transportType());
        long nowEpochMs = System.currentTimeMillis();
        return interactionStore().markDispatching(
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
                .onFailure().call(failure -> interactionStore().fail(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    failure.getMessage(),
                    System.currentTimeMillis())
                    .replaceWith(failure))
                .onItem().transformToUni(result -> interactionStore().markDispatched(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    result.metadata(),
                    System.currentTimeMillis()))
                .onItem().transform(optional -> optional.orElseThrow(() ->
                    new IllegalStateException("Await interaction metadata update lost OCC race: "
                        + claimedInteraction.interactionId()))));
    }

    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        AwaitCompletionCommand normalized = normalizeCompletionCommand(command);
        if (normalized.resumeToken() == null) {
            return interactionStore().complete(normalized);
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
            .onItem().transformToUni(ignored -> interactionStore().complete(normalized));
    }

    public Uni<AwaitUnitRecord> recordCompletion(AwaitInteractionRecord record, long nowEpochMs) {
        if (record.status() == AwaitInteractionStatus.TIMED_OUT) {
            return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.TIMED_OUT, nowEpochMs)
                .onItem().transform(optional -> optional.orElseThrow(
                    () -> new IllegalStateException("Await unit not found for timed-out interaction " + record.unitId())));
        }
        if (record.status() == AwaitInteractionStatus.FAILED) {
            return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.FAILED, nowEpochMs)
                .onItem().transform(optional -> optional.orElseThrow(
                    () -> new IllegalStateException("Await unit not found for failed interaction " + record.unitId())));
        }
        Uni<Optional<AwaitUnitRecord>> updated = record.itemInteraction()
            ? unitStore().recordItemCompleted(record.tenantId(), record.unitId(), nowEpochMs)
            : unitStore().markCompleted(record.tenantId(), record.unitId(), nowEpochMs);
        return updated.onItem().transform(optional -> optional.orElseThrow(
            () -> new IllegalStateException("Await unit not found while recording completion: " + record.unitId())));
    }

    public Uni<AwaitUnitRecord> markDispatchComplete(String tenantId, String unitId, int expectedItemCount, long nowEpochMs) {
        return unitStore().markDispatchComplete(tenantId, unitId, expectedItemCount, nowEpochMs)
            .onItem().transform(optional -> optional.orElseThrow(
                () -> new IllegalStateException("Await unit not found while completing dispatch: " + unitId)));
    }

    public Uni<AwaitUnitRecord> getUnit(String tenantId, String unitId) {
        return unitStore().get(tenantId, unitId)
            .onItem().transform(optional -> optional.orElseThrow(
                () -> new AwaitInteractionNotFoundException("No await unit matches id " + unitId)));
    }

    public Uni<Object> loadResumePayload(String tenantId, String unitId) {
        return getUnit(tenantId, unitId).onItem().transformToUni(unit -> {
            if (unit.primaryInteractionId() != null) {
                return interactionStore().get(tenantId, unit.primaryInteractionId())
                    .onItem().transform(optional -> optional.orElseThrow(
                        () -> new IllegalStateException("Await interaction not found for primary interaction id "
                            + unit.primaryInteractionId())))
                    .onItem().transform(this::coerceResumePayload);
            }
            return interactionStore().findByUnit(tenantId, unitId)
                .onItem().transform(records -> List.copyOf(records.stream()
                    .sorted(java.util.Comparator.comparing(record -> record.itemIndex() == null ? Integer.MAX_VALUE : record.itemIndex()))
                    .map(this::coerceResumePayload)
                    .toList()));
        });
    }

    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return interactionStore().queryPending(tenantId, assignee, group, stepId, limit);
    }

    public Uni<List<AwaitInteractionRecord>> findByUnit(String tenantId, String unitId) {
        return interactionStore().findByUnit(tenantId, unitId);
    }

    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return interactionStore().findTimedOut(nowEpochMs, limit);
    }

    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(AwaitInteractionRecord record, long nowEpochMs) {
        return interactionStore().markTimedOut(record.tenantId(), record.interactionId(), record.version(), nowEpochMs)
            .onItem().transformToUni(updated -> {
                if (updated.isEmpty()) {
                    return Uni.createFrom().item(Optional.empty());
                }
                return unitStore().markTerminal(
                        record.tenantId(),
                        record.unitId(),
                        AwaitUnitStatus.TIMED_OUT,
                        nowEpochMs)
                    .replaceWith(updated);
            });
    }

    private Uni<AwaitCreateResult> createInteraction(
        AwaitStepDescriptor descriptor,
        String unitId,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        Integer itemIndex,
        String assignee,
        String group
    ) {
        Object normalizedRequestPayload = AwaitPayloadSupport.normalize(requestPayload);
        long now = System.currentTimeMillis();
        long deadline = now + descriptor.timeout().toMillis();
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String idempotencyKey = deriveIdempotencyKey(descriptor, executionId, normalizedRequestPayload)
            + (itemIndex == null ? "" : ":item=" + itemIndex);
        String correlationId = deriveCorrelationId(descriptor, tenantId, executionId, idempotencyKey);
        return interactionStore().createOrGet(new AwaitCreateCommand(
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
            unitId,
            itemIndex,
            now,
            deadline,
            ttl));
    }

    private Uni<AwaitUnitRecord> createOrGetUnit(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String unitId,
        String executionId,
        int stepIndex
    ) {
        long now = System.currentTimeMillis();
        long ttl = Instant.ofEpochMilli(now + descriptor.timeout().toMillis()).plusSeconds(86_400).getEpochSecond();
        return unitStore().createOrGet(new AwaitUnitCreateCommand(
            tenantId,
            unitId,
            executionId,
            descriptor.stepId(),
            stepIndex,
            descriptor.cardinality(),
            now,
            ttl));
    }

    private AwaitInteractionStore interactionStore() {
        AwaitInteractionStore cached = resolvedInteractionStore;
        if (cached != null) {
            return cached;
        }
        String provider = orchestratorConfig == null ? null : orchestratorConfig.stateProvider();
        synchronized (this) {
            if (resolvedInteractionStore == null) {
                resolvedInteractionStore = interactionStores.stream()
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
            return resolvedInteractionStore;
        }
    }

    private AwaitUnitStore unitStore() {
        AwaitUnitStore cached = resolvedUnitStore;
        if (cached != null) {
            return cached;
        }
        String provider = orchestratorConfig == null ? null : orchestratorConfig.stateProvider();
        synchronized (this) {
            if (resolvedUnitStore == null) {
                resolvedUnitStore = unitStores.stream()
                    .filter(candidate -> provider == null || provider.isBlank() || provider.equalsIgnoreCase(candidate.providerName()))
                    .sorted((left, right) -> {
                        int priorityComparison = Integer.compare(right.priority(), left.priority());
                        if (priorityComparison != 0) {
                            return priorityComparison;
                        }
                        return left.providerName().compareToIgnoreCase(right.providerName());
                    })
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No AwaitUnitStore provider is available"
                        + (provider == null || provider.isBlank() ? "" : " for provider " + provider)));
            }
            return resolvedUnitStore;
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
        Uni<Optional<AwaitInteractionRecord>> lookup = command.interactionId() != null
            ? interactionStore().get(command.tenantId(), command.interactionId())
            : interactionStore().findByCorrelation(command.tenantId(), command.correlationId());
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

    private Object coerceResumePayload(AwaitInteractionRecord record) {
        try {
            Class<?> outputType = AwaitPayloadSupport.resolvePayloadClass(
                record.outputType(),
                Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(record.responsePayload(), outputType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed resolving await output type " + record.outputType()
                    + " for interaction " + record.interactionId(),
                e);
        }
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

    private static String deriveUnitId(String tenantId, String executionId, String stepId, int stepIndex) {
        String basis = tenantId + ":" + executionId + ":" + stepId + ":" + stepIndex;
        return UUID.nameUUIDFromBytes(basis.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    }
}
