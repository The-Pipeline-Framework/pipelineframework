package org.pipelineframework.awaitable;

import java.time.Instant;
import java.util.ArrayList;
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
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineTelemetry;

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

    @Inject
    PipelineTelemetry telemetry;

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
                .onItem().transformToUni(optional -> optional
                    .map(Uni.createFrom()::item)
                    .orElseGet(() -> resolvedAfterDispatchMetadataRace(claimedInteraction)))
                .onItem().invoke(this::recordInteractionDispatched));
    }

    private Uni<AwaitInteractionRecord> resolvedAfterDispatchMetadataRace(AwaitInteractionRecord claimedInteraction) {
        return interactionStore().get(claimedInteraction.tenantId(), claimedInteraction.interactionId())
            .onItem().transform(optional -> {
                if (optional.isPresent() && optional.get().status().terminal()) {
                    return optional.get();
                }
                throw new IllegalStateException("Await interaction metadata update lost OCC race: "
                    + claimedInteraction.interactionId());
            });
    }

    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        AwaitCompletionCommand normalized = normalizeCompletionCommand(command);
        return resolveForCompletion(normalized)
            .onItem().transformToUni(record -> enforceCompletionPayloadLimitIfUnitPresent(record, normalized)
                .onItem().transform(safeCommand -> new ValidatedCompletion(record, safeCommand)))
            .onItem().transformToUni(validated -> {
                AwaitCompletionCommand safeCommand = validated.command();
                AwaitInteractionRecord record = validated.record();
                if (safeCommand.resumeToken() == null) {
                    return Uni.createFrom().item(withResolvedInteractionId(safeCommand, record));
                }
                if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
                    return Uni.createFrom().failure(
                        new AwaitInteractionTerminalException("Await interaction is terminal: " + record.status()));
                }
                return Uni.createFrom().item(() -> {
                        resumeTokenService.validate(safeCommand.resumeToken(), record, safeCommand.nowEpochMs());
                        return safeCommand;
                    })
                    .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                    .replaceWith(withResolvedInteractionId(safeCommand, record));
            })
            .onItem().transformToUni(safeCommand -> interactionStore().complete(safeCommand));
    }

    private static AwaitCompletionCommand withResolvedInteractionId(
        AwaitCompletionCommand command,
        AwaitInteractionRecord record
    ) {
        if (command.interactionId() != null) {
            return command;
        }
        return new AwaitCompletionCommand(
            command.tenantId(),
            record.interactionId(),
            command.correlationId(),
            command.resumeToken(),
            command.idempotencyKey(),
            command.responsePayload(),
            command.actor(),
            command.nowEpochMs());
    }

    private Uni<AwaitCompletionCommand> enforceCompletionPayloadLimitIfUnitPresent(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command
    ) {
        return unitStore().get(record.tenantId(), record.unitId())
            .onItem().transform(optional -> {
                if (optional.isEmpty()) {
                    return command;
                }
                Object safePayload = validateAggregateOutputLimit(optional.get(), command.responsePayload());
                return withResponsePayload(command, safePayload);
            });
    }

    public Uni<AwaitUnitRecord> recordCompletion(AwaitInteractionRecord record, long nowEpochMs) {
        if (record.status() == AwaitInteractionStatus.TIMED_OUT) {
            return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.TIMED_OUT, nowEpochMs)
                .onItem().transform(optional -> optional.orElseThrow(
                    () -> new IllegalStateException("Await unit not found for timed-out interaction " + record.unitId())))
                .onItem().invoke(unit -> recordUnitTerminal(record, unit));
        }
        if (record.status() == AwaitInteractionStatus.FAILED) {
            return unitStore().markTerminal(record.tenantId(), record.unitId(), AwaitUnitStatus.FAILED, nowEpochMs)
                .onItem().transform(optional -> optional.orElseThrow(
                    () -> new IllegalStateException("Await unit not found for failed interaction " + record.unitId())))
                .onItem().invoke(unit -> recordUnitTerminal(record, unit));
        }
        Uni<Optional<AwaitUnitRecord>> updated = record.itemInteraction()
            ? unitStore().recordItemCompleted(record.tenantId(), record.unitId(), itemCompletionKey(record), nowEpochMs)
            : unitStore().markCompleted(record.tenantId(), record.unitId(), nowEpochMs);
        return updated.onItem().transform(optional -> optional.orElseThrow(
            () -> new IllegalStateException("Await unit not found while recording completion: " + record.unitId())))
            .onItem().invoke(unit -> recordCompletionLifecycle(record, unit));
    }

    private static String itemCompletionKey(AwaitInteractionRecord record) {
        return record.itemIndex() == null ? record.interactionId() : "item:" + record.itemIndex();
    }

    public Uni<AwaitUnitRecord> markDispatchComplete(String tenantId, String unitId, int expectedItemCount, long nowEpochMs) {
        return unitStore().markDispatchComplete(tenantId, unitId, expectedItemCount, nowEpochMs)
            .onItem().transform(optional -> optional.orElseThrow(
                () -> new IllegalStateException("Await unit not found while completing dispatch: " + unitId)))
            .onItem().invoke(this::recordUnitDispatchComplete);
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
                    .onItem().transform(record -> enforceAggregateOutputLimit(unit, coerceResumePayload(record)));
            }
            return interactionStore().findByUnit(tenantId, unitId)
                .onItem().transform(records -> {
                    var completedByItem = records.stream()
                        .filter(record -> record.status() == AwaitInteractionStatus.COMPLETED)
                        .collect(java.util.stream.Collectors.groupingBy(
                            record -> record.itemIndex() == null ? Integer.valueOf(-1) : record.itemIndex()));
                    return completedByItem.entrySet().stream()
                        .sorted(java.util.Map.Entry.comparingByKey())
                        .map(entry -> {
                            var group = entry.getValue();
                            return group.stream()
                                .max(java.util.Comparator.comparing(AwaitInteractionRecord::createdAtEpochMs)
                                    .thenComparing(AwaitInteractionRecord::version))
                                .orElseThrow();
                        })
                        .map(this::coerceResumePayload)
                        .toList();
                });
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

    public Uni<TransitionAwaitSuspension> suspensionSnapshot(AwaitSuspendedException suspended) {
        return getUnit(suspended.tenantId(), suspended.unitId())
            .onItem().transformToUni(unit -> findByUnit(suspended.tenantId(), suspended.unitId())
                .onItem().transform(interactions -> new TransitionAwaitSuspension(
                    suspended.tenantId(),
                    suspended.executionId(),
                    suspended.unitId(),
                    suspended.stepIndex(),
                    unit,
                    interactions)));
    }

    public Uni<Void> importSuspension(TransitionAwaitSuspension suspension) {
        if (suspension == null || suspension.unit() == null) {
            return Uni.createFrom().voidItem();
        }
        List<Uni<Void>> imports = new ArrayList<>();
        imports.add(unitStore().importRecord(suspension.unit()).replaceWithVoid());
        for (AwaitInteractionRecord interaction : suspension.interactions()) {
            imports.add(interactionStore().importRecord(interaction).replaceWithVoid());
        }
        return Uni.join().all(imports).andCollectFailures().replaceWithVoid();
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
                    .onItem().invoke(unit -> unit.ifPresent(value -> recordUnitTerminal(updated.get(), value)))
                    .replaceWith(updated);
            });
    }

    private void recordInteractionDispatched(AwaitInteractionRecord record) {
        AwaitCompletionMetrics.recordInteractionDispatched(record);
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.INTERACTION_DISPATCHED,
            record.executionId(),
            record.unitId(),
            record.stepId(),
            record.stepIndex(),
            record.status().name(),
            record.interactionId(),
            record.correlationId(),
            record.transportType(),
            record.itemIndex(),
            null,
            null,
            null));
    }

    private void recordUnitDispatchComplete(AwaitUnitRecord unit) {
        AwaitCompletionMetrics.recordUnitDispatchComplete(unit);
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.UNIT_DISPATCH_COMPLETE,
            unit.executionId(),
            unit.unitId(),
            unit.stepId(),
            unit.stepIndex(),
            unit.status().name(),
            unit.primaryInteractionId(),
            null,
            null,
            null,
            unit.expectedItemCount(),
            unit.completedItemCount(),
            unit.dispatchComplete()));
    }

    private void recordCompletionLifecycle(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        AwaitCompletionMetrics.recordCompletionAdmitted(record);
        if (record.itemInteraction()) {
            AwaitCompletionMetrics.recordItemCompleted(record, unit);
            recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
                AwaitReplayLifecycleEvent.UNIT_ITEM_COMPLETED,
                record.executionId(),
                record.unitId(),
                record.stepId(),
                record.stepIndex(),
                unit.status().name(),
                record.interactionId(),
                record.correlationId(),
                record.transportType(),
                record.itemIndex(),
                unit.expectedItemCount(),
                unit.completedItemCount(),
                unit.dispatchComplete()));
        }
        if (unit.status() == AwaitUnitStatus.COMPLETED) {
            recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
                AwaitReplayLifecycleEvent.UNIT_COMPLETED,
                unit.executionId(),
                unit.unitId(),
                unit.stepId(),
                unit.stepIndex(),
                unit.status().name(),
                record.interactionId(),
                record.correlationId(),
                record.transportType(),
                record.itemIndex(),
                unit.expectedItemCount(),
                unit.completedItemCount(),
                unit.dispatchComplete()));
        }
    }

    private void recordUnitTerminal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        AwaitCompletionMetrics.recordUnitTerminal(record, unit);
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            AwaitReplayLifecycleEvent.UNIT_TERMINAL,
            unit.executionId(),
            unit.unitId(),
            unit.stepId(),
            unit.stepIndex(),
            unit.status().name(),
            record.interactionId(),
            record.correlationId(),
            record.transportType(),
            record.itemIndex(),
            unit.expectedItemCount(),
            unit.completedItemCount(),
            unit.dispatchComplete()));
    }

    private void recordAwaitLifecycle(AwaitReplayLifecycleEvent lifecycleEvent) {
        if (telemetry != null) {
            telemetry.recordAwaitLifecycle(lifecycleEvent);
        }
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
        Uni<Optional<AwaitInteractionRecord>> lookup;
        if (command.interactionId() != null) {
            lookup = interactionStore().get(command.tenantId(), command.interactionId());
        } else if (command.correlationId() != null) {
            lookup = interactionStore().findByCorrelation(command.tenantId(), command.correlationId());
        } else {
            lookup = interactionStore().get(command.tenantId(), resumeTokenService.interactionIdHint(command.resumeToken()));
        }
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

    private Object enforceAggregateOutputLimit(AwaitUnitRecord unit, Object payload) {
        return checkAndMaterializeAggregateOutput(unit, payload);
    }

    private Object validateAggregateOutputLimit(AwaitUnitRecord unit, Object payload) {
        return checkAndMaterializeAggregateOutput(unit, payload);
    }

    private Object checkAndMaterializeAggregateOutput(AwaitUnitRecord unit, Object payload) {
        if (!materializedOutputCardinality(unit.cardinality())) {
            return payload;
        }
        int configuredLimit = orchestratorConfig == null ? 0 : orchestratorConfig.awaitAggregateMaxOutputItems();
        if (configuredLimit <= 0 || payload == null) {
            return payload;
        }
        if (payload instanceof Iterable<?> iterable) {
            List<Object> materialized = new ArrayList<>();
            for (Object item : iterable) {
                if (materialized.size() == configuredLimit) {
                    throw aggregateOutputLimitFailure(unit, configuredLimit + 1, configuredLimit);
                }
                materialized.add(item);
            }
            return List.copyOf(materialized);
        }
        if (payload.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(payload);
            if (length > configuredLimit) {
                throw aggregateOutputLimitFailure(unit, length, configuredLimit);
            }
            return payload;
        }
        return payload;
    }

    private static AwaitCompletionCommand withResponsePayload(AwaitCompletionCommand command, Object responsePayload) {
        return new AwaitCompletionCommand(
            command.tenantId(),
            command.interactionId(),
            command.correlationId(),
            command.resumeToken(),
            command.idempotencyKey(),
            responsePayload,
            command.actor(),
            command.nowEpochMs());
    }

    private record ValidatedCompletion(AwaitInteractionRecord record, AwaitCompletionCommand command) {
    }

    private static boolean materializedOutputCardinality(String cardinality) {
        return "ONE_TO_MANY".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality);
    }

    private static IllegalStateException aggregateOutputLimitFailure(
        AwaitUnitRecord unit,
        int observedCount,
        int configuredLimit
    ) {
        return new IllegalStateException(
            "Await unit " + unit.unitId()
                + " materialized at least " + observedCount + " output items for "
                + unit.cardinality()
                + ", exceeding pipeline.orchestrator.await-aggregate-max-output-items="
                + configuredLimit + ".");
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
