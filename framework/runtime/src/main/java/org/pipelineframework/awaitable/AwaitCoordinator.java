package org.pipelineframework.awaitable;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineTelemetry;

/**
 * Coordinates await unit persistence, interaction dispatch, completion admission, and replay payload loading.
 */
@ApplicationScoped
public class AwaitCoordinator {
    private static final Logger LOG = Logger.getLogger(AwaitCoordinator.class);

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
    AwaitAdmissionCoordinator awaitAdmissionCoordinator;

    @Inject
    AwaitStepDescriptorFactory descriptorFactory;

    @Inject
    AwaitDurablePayloadResolver durablePayloadResolver;

    @Inject
    PipelineTelemetry telemetry;

    private volatile AwaitInteractionStore resolvedInteractionStore;
    private volatile AwaitUnitStore resolvedUnitStore;
    private final Map<String, AwaitTransportAdapter<?>> resolvedAdapters = new ConcurrentHashMap<>();
    private final Map<String, AwaitStepDescriptor> directDescriptors = new ConcurrentHashMap<>();

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
        return registerDescriptor(descriptor)
            .chain(() -> createOrGetUnit(descriptor, tenantId, unitId, executionId, stepIndex))
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
        return registerDescriptor(descriptor)
            .onItem().transformToUni(ignored -> {
                if (itemIndex < 0) {
                    return Uni.createFrom().failure(new IllegalArgumentException("itemIndex must be non-negative"));
                }
                return createOrGetUnit(descriptor, tenantId, unitId, executionId, stepIndex)
                    .onItem().transformToUni(unit -> createItemInPreparedUnit(
                        descriptor, unitId, tenantId, executionId, stepIndex, causationId, requestPayload,
                        itemIndex, assignee, group));
            });
    }

    /** Creates the durable unit once before a live itemized source begins concurrent dispatch. */
    public Uni<Void> prepareLiveItemizedUnit(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String unitId,
        String executionId,
        int stepIndex
    ) {
        return registerDescriptor(descriptor)
            .chain(() -> createOrGetUnit(descriptor, tenantId, unitId, executionId, stepIndex))
            .replaceWithVoid();
    }

    /** Creates one item in a unit that was durably prepared by the live stream before source demand began. */
    public Uni<AwaitCreateResult> createOrGetPreparedItem(
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
        return registerDescriptor(descriptor)
            .chain(() -> {
                if (itemIndex < 0) {
                    return Uni.createFrom().failure(new IllegalArgumentException("itemIndex must be non-negative"));
                }
                return createItemInPreparedUnit(
                    descriptor, unitId, tenantId, executionId, stepIndex, causationId, requestPayload,
                    itemIndex, assignee, group);
            });
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
            .onItem().transformToUni(claimedInteraction -> AwaitCompletionMetrics.inProviderDispatchSpan(
                claimedInteraction,
                () -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                    descriptor,
                    claimedInteraction,
                    transportRequestPayload(descriptor, claimedInteraction))))
                .onFailure().call(failure -> interactionStore().fail(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    failure.getMessage(),
                    System.currentTimeMillis())
                    .onItem().transformToUni(updated -> updated
                        .map(this::releaseAdmission)
                        .orElseGet(() -> Uni.createFrom().voidItem()))
                    .replaceWithVoid())
                .onItem().transformToUni(result -> interactionStore().markDispatched(
                    claimedInteraction.tenantId(),
                    claimedInteraction.interactionId(),
                    claimedInteraction.version(),
                    dispatchMetadata(claimedInteraction, result.metadata()),
                    System.currentTimeMillis()))
                .onItem().transformToUni(optional -> optional
                    .map(Uni.createFrom()::item)
                    .orElseGet(() -> resolvedAfterDispatchMetadataRace(claimedInteraction)))
                .onItem().invoke(this::recordInteractionDispatched));
    }

    /**
     * Dispatches an interaction owned by an active live stream.  The durable dispatch intent,
     * including the admission-reservation identity needed by a later retry, is committed before
     * sending.  Unlike the conservative handoff path, live dispatch does not synchronously wait
     * for a second post-send metadata transition: a completion may legally race the send and
     * complete a {@code DISPATCHING} interaction.
     */
    @SuppressWarnings("unchecked")
    public Uni<AwaitInteractionRecord> dispatchLive(AwaitStepDescriptor descriptor, AwaitInteractionRecord interaction) {
        AwaitTransportAdapter<Object> adapter = (AwaitTransportAdapter<Object>) adapter(descriptor.transportType());
        Uni<AwaitInteractionRecord> intended = interaction.status() == AwaitInteractionStatus.WAITING
            ? interactionStore().markDispatching(
                interaction.tenantId(),
                interaction.interactionId(),
                interaction.version(),
                dispatchMetadata(interaction, Map.of()),
                System.currentTimeMillis())
                .onItem().transform(optional -> optional.orElseThrow(() ->
                    new IllegalStateException("Await interaction live dispatch transition lost OCC race: "
                        + interaction.interactionId())))
            : Uni.createFrom().item(interaction);
        return intended.onItem().transformToUni(dispatching -> AwaitCompletionMetrics.inProviderDispatchSpan(
            dispatching,
            () -> adapter.dispatch(new AwaitTransportAdapter.AwaitDispatchRequest<>(
                descriptor,
                dispatching,
                transportRequestPayload(descriptor, dispatching))))
            .onFailure().call(failure -> interactionStore().fail(
                dispatching.tenantId(),
                dispatching.interactionId(),
                dispatching.version(),
                failure.getMessage(),
                System.currentTimeMillis())
                .onItem().transformToUni(updated -> updated
                    .map(this::releaseAdmission)
                    .orElseGet(() -> Uni.createFrom().voidItem()))
                .replaceWithVoid())
            .replaceWith(dispatching)
            .invoke(this::recordInteractionDispatched));
    }

    private Map<String, Object> dispatchMetadata(AwaitInteractionRecord interaction, Map<String, Object> metadata) {
        Map<String, Object> merged = new java.util.LinkedHashMap<>(interaction.transportMetadata());
        if (metadata != null) {
            merged.putAll(metadata);
        }
        return awaitAdmissionCoordinator == null ? Map.copyOf(merged)
            : awaitAdmissionCoordinator.dispatchMetadata(interaction, Map.copyOf(merged));
    }

    /**
     * Resolves whether an await transport can participate in a live pending window.
     *
     * @param descriptor await descriptor
     * @return true when the transport feeds the live completion registry
     */
    public boolean supportsLiveAwaitWindow(AwaitStepDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        return adapter(descriptor.transportType()).supportsLiveAwaitWindow(descriptor);
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
            .onItem().transformToUni(record -> validateCompletionAdmission(record, normalized)
                .onItem().transform(safeCommand -> completionContract(record, safeCommand)))
            .onItem().transformToUni(this::enforceCompletionPayloadLimit)
            .onItem().transformToUni(validated -> interactionStore().complete(validated.command()));
    }

    private Uni<AwaitCompletionCommand> validateCompletionAdmission(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command
    ) {
        if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
            return Uni.createFrom().failure(
                new AwaitInteractionTerminalException("Await interaction is terminal: " + record.status()));
        }
        if (command.resumeToken() == null) {
            return Uni.createFrom().item(withResolvedInteractionId(command, record));
        }
        return Uni.createFrom().item(() -> {
                resumeTokenService.validate(command.resumeToken(), record, command.nowEpochMs());
                return withResolvedInteractionId(command, record);
            })
            .runSubscriptionOn(Infrastructure.getDefaultExecutor());
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

    private Uni<ValidatedCompletion> enforceCompletionPayloadLimit(
        ValidatedCompletion validated
    ) {
        if (!materializedOutputCardinality(validated.descriptor().cardinality())) {
            return Uni.createFrom().item(validated);
        }
        return unitStore().get(validated.record().tenantId(), validated.record().unitId())
            .onItem().transform(optional -> {
                if (optional.isEmpty()) {
                    return validated;
                }
                Object safePayload = validateAggregateOutputLimit(optional.get(), validated.command().responsePayload());
                return new ValidatedCompletion(
                    validated.record(),
                    withResponsePayload(validated.command(), safePayload),
                    validated.descriptor());
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

    /**
     * Rebuilds the conservative itemized-await aggregate from durable interaction facts.
     * This is intentionally used only after a stream has entered the no-live-owner fallback.
     */
    public Uni<Void> reconcileCompletedItemInteractions(String tenantId, String unitId, long nowEpochMs) {
        return findByUnit(tenantId, unitId)
            .onItem().transformToMulti(records -> Multi.createFrom().iterable(records))
            .select().where(record -> record.itemInteraction() && record.status() == AwaitInteractionStatus.COMPLETED)
            .onItem().transformToUniAndConcatenate(record -> recordCompletion(record, nowEpochMs).replaceWithVoid())
            .collect().last()
            .replaceWithVoid();
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

    public Uni<AwaitUnitRecord> recordItemContinuationCompleted(
        String tenantId,
        String unitId,
        int itemIndex,
        long nowEpochMs
    ) {
        return unitStore().recordItemContinuationCompleted(
                tenantId,
                unitId,
                AwaitUnitRecord.continuationCompletionKey(itemIndex),
                nowEpochMs)
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
                    .onItem().transform(record -> enforceAggregateOutputLimit(unit, resumePayload(record)));
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
                                .map(this::resumePayload)
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

    /** Preloads release-pinned payload metadata before concurrent item dispatch begins. */
    public Uni<Void> preloadDurablePayloads(String tenantId, String executionId) {
        return durablePayloadResolver == null
            ? Uni.createFrom().voidItem()
            : durablePayloadResolver.preload(tenantId, executionId);
    }

    public Uni<TransitionAwaitSuspension> suspensionSnapshot(AwaitSuspendedException suspended) {
        return getUnit(suspended.tenantId(), suspended.unitId())
            .onItem().transformToUni(unit -> findByUnit(suspended.tenantId(), suspended.unitId())
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .onItem().transform(interactions -> new TransitionAwaitSuspension(
                    suspended.tenantId(),
                    suspended.executionId(),
                    suspended.unitId(),
                    suspended.stepIndex(),
                    unit,
                    interactions.stream().map(this::transportSafeSnapshot).toList())));
    }

    private AwaitInteractionRecord transportSafeSnapshot(AwaitInteractionRecord interaction) {
        if (durablePayloadResolver != null && durablePayloadResolver.supportsTypedPayloads(interaction)) {
            return interaction.withPayloadSnapshots(
                typedSnapshot(interaction, AwaitDurablePayloadResolver.Slot.REQUEST, interaction.requestPayload()),
                typedSnapshot(interaction, AwaitDurablePayloadResolver.Slot.RESPONSE, interaction.responsePayload()));
        }
        return interaction.withPayloadSnapshots(
            AwaitPayloadSupport.normalize(interaction.requestPayload()),
            AwaitPayloadSupport.normalize(interaction.responsePayload()));
    }

    private TypedDurablePayload typedSnapshot(
        AwaitInteractionRecord interaction,
        AwaitDurablePayloadResolver.Slot slot,
        Object payload
    ) {
        if (payload == null) {
            return null;
        }
        return TypedDurablePayload.fromSerializedBytes(
            durablePayloadResolver.encode(interaction, slot, payload).getBytes(StandardCharsets.UTF_8))
            .orElseThrow(() -> new IllegalStateException("Await typed snapshot encoding failed for " + interaction.interactionId()));
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
                    .chain(() -> releaseAdmission(updated.get()).replaceWith(updated));
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

    private void recordAdmissionLifecycle(String eventName, AwaitInteractionRecord record) {
        recordAwaitLifecycle(new AwaitReplayLifecycleEvent(
            eventName,
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
        Object canonicalRequestPayload = restoreCanonicalRequestPayload(descriptor, requestPayload);
        long now = System.currentTimeMillis();
        long deadline = now + descriptor.timeout().toMillis();
        long ttl = Instant.ofEpochMilli(deadline).plusSeconds(86_400).getEpochSecond();
        String idempotencyKey = deriveIdempotencyKey(descriptor, executionId, canonicalRequestPayload)
            + (itemIndex == null ? "" : ":item=" + itemIndex);
        String correlationId = deriveCorrelationId(descriptor, tenantId, executionId, idempotencyKey);
        return acquireAdmission(descriptor, tenantId, unitId, itemIndex, executionId, deadline)
            .onItem().transformToUni(lease -> interactionStore().createOrGet(new AwaitCreateCommand(
                tenantId,
                executionId,
                descriptor.stepId(),
                stepIndex,
                descriptor.outputType(),
                descriptor.transportOutputType(),
                causationId,
                idempotencyKey,
                correlationId,
                canonicalRequestPayload,
                assignee,
                group,
                descriptor.transportType(),
                unitId,
                itemIndex,
                telemetry == null
                    ? AwaitCompletionMetrics.captureTraceMetadata()
                    : AwaitCompletionMetrics.captureTraceMetadata(telemetry.activeRunSpanContext()),
                now,
                deadline,
                ttl))
                .onItem().transformToUni(created -> bindOrReleaseAdmission(created, lease))
                .onItem().invoke(created -> {
                    if (!created.duplicate()) {
                        AwaitCompletionMetrics.recordInteractionCreated(created.record());
                    }
                })
                .onFailure().call(ignored -> releaseAdmissionAfterDefiniteCreateFailure(lease, tenantId, correlationId)));
    }

    private Uni<AwaitCreateResult> createItemInPreparedUnit(
        AwaitStepDescriptor descriptor,
        String unitId,
        String tenantId,
        String executionId,
        int stepIndex,
        String causationId,
        Object requestPayload,
        int itemIndex,
        String assignee,
        String group
    ) {
        return createInteraction(
            descriptor, unitId, tenantId, executionId, stepIndex, causationId, requestPayload,
            itemIndex, assignee, group);
    }

    private static Object restoreCanonicalRequestPayload(AwaitStepDescriptor descriptor, Object requestPayload) {
        try {
            Class<?> canonicalType = AwaitPayloadSupport.resolvePayloadClass(
                descriptor.inputType(), Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(requestPayload, canonicalType);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed restoring await request payload to canonical type "
                    + descriptor.inputType() + " for step " + descriptor.stepId(),
                e);
        }
    }

    /**
     * The durable interaction owns the canonical request. The representation adapter is deliberately
     * applied only at dispatch, so protobuf/JSON transport values cannot become replay inputs.
     */
    private static Object transportRequestPayload(
        AwaitStepDescriptor descriptor,
        AwaitInteractionRecord interaction
    ) {
        Object canonical = restoreCanonicalRequestPayload(descriptor, interaction.requestPayload());
        return AwaitPayloadSupport.normalize(descriptor.inputToTransport().apply(canonical));
    }

    /**
     * A retry can acquire a new lease before discovering that its interaction already completed.
     * That lease never represented provider work, so release it instead of retaining it until TTL.
     */
    private Uni<AwaitCreateResult> bindOrReleaseAdmission(
        AwaitCreateResult created,
        Optional<AwaitAdmissionCoordinator.AdmissionLease> lease
    ) {
        if (awaitAdmissionCoordinator == null || lease.isEmpty()) {
            return Uni.createFrom().item(created);
        }
        if (created.duplicate() && created.record().status().terminal()) {
            return awaitAdmissionCoordinator.release(lease.orElseThrow().reservation())
                .onFailure().invoke(failure -> LOG.warnf(
                    failure,
                    "Failed releasing duplicate terminal await admission lease interactionId=%s",
                    created.record().interactionId()))
                .onFailure().recoverWithUni(failure -> Uni.createFrom().voidItem())
                .replaceWith(created);
        }
        bindAdmission(created.record(), lease);
        return Uni.createFrom().item(created);
    }

    /**
     * A failed create may have committed before the client observed the error. Retain the
     * lease unless a durable correlation lookup proves that no interaction exists.
     */
    private Uni<Void> releaseAdmissionAfterDefiniteCreateFailure(
        Optional<AwaitAdmissionCoordinator.AdmissionLease> lease,
        String tenantId,
        String correlationId
    ) {
        if (awaitAdmissionCoordinator == null || lease.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return interactionStore().findByCorrelation(tenantId, correlationId)
            .onItem().transformToUni(existing -> existing.isEmpty()
                ? awaitAdmissionCoordinator.release(lease.orElseThrow().reservation()).replaceWithVoid()
                : Uni.createFrom().voidItem())
            // An unavailable or failed lookup is ambiguous, so TTL/reconciliation owns recovery.
            .onFailure().recoverWithUni(Uni.createFrom().voidItem());
    }

    private Uni<Optional<AwaitAdmissionCoordinator.AdmissionLease>> acquireAdmission(
        AwaitStepDescriptor descriptor,
        String tenantId,
        String unitId,
        Integer itemIndex,
        String executionId,
        long deadlineEpochMs
    ) {
        return awaitAdmissionCoordinator == null
            ? Uni.createFrom().item(Optional.empty())
            : awaitAdmissionCoordinator.acquire(descriptor, tenantId, unitId, itemIndex, executionId, deadlineEpochMs);
    }

    private void bindAdmission(AwaitInteractionRecord interaction, Optional<AwaitAdmissionCoordinator.AdmissionLease> lease) {
        if (awaitAdmissionCoordinator != null) {
            lease.ifPresent(activeLease -> awaitAdmissionCoordinator.bind(interaction, activeLease));
            if (lease.isPresent()) {
                AwaitAdmissionCoordinator.AdmissionLease activeLease = lease.orElseThrow();
                recordAdmissionLifecycle(
                    activeLease.reused() ? AwaitReplayLifecycleEvent.ADMISSION_REUSED : AwaitReplayLifecycleEvent.ADMISSION_ACQUIRED,
                    interaction);
                if (activeLease.reconciledExpired()) {
                    recordAdmissionLifecycle(AwaitReplayLifecycleEvent.ADMISSION_RECONCILED, interaction);
                }
            }
        }
    }

    /**
     * Releases the provider-facing reservation after a durable completion handoff.
     */
    public Uni<Void> releaseAdmission(AwaitInteractionRecord interaction) {
        if (!admissionEnabled()) {
            return Uni.createFrom().voidItem();
        }
        return awaitAdmissionCoordinator == null
            ? Uni.createFrom().voidItem()
            : awaitAdmissionCoordinator.release(interaction)
                .invoke(released -> {
                    if (released) {
                        recordAdmissionLifecycle(AwaitReplayLifecycleEvent.ADMISSION_RELEASED, interaction);
                    }
                })
                .onFailure().invoke(failure -> LOG.warnf(failure,
                    "Await admission release bookkeeping failed interactionId=%s", interaction.interactionId()))
                .onFailure().recoverWithItem(false)
                .replaceWithVoid();
    }

    public boolean admissionEnabled() {
        return awaitAdmissionCoordinator != null && awaitAdmissionCoordinator.enabled();
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

    /**
     * Decodes one durable await interaction's transport payload and returns its canonical pipeline value.
     *
     * <p>The durable record preserves the canonical contract separately from the transport representation.
     * This method validates both identities against the rebuilt step descriptor before applying the generated
     * transport-to-canonical adapter.</p>
     *
     * @param record completed durable interaction
     * @return canonical pipeline payload
     */
    public Object resumePayload(AwaitInteractionRecord record) {
        try {
            AwaitStepDescriptor descriptor = descriptorFor(record);
            validateDurableOutputContract(record, descriptor);
            Class<?> canonicalOutputType = AwaitPayloadSupport.resolvePayloadClass(
                record.outputType(),
                Thread.currentThread().getContextClassLoader());
            if (canonicalOutputType.isInstance(record.responsePayload())) {
                return record.responsePayload();
            }
            return canonicalCompletionPayload(record, descriptor, record.responsePayload());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed resolving await canonical output type " + record.outputType()
                    + " for interaction " + record.interactionId() + " stepId=" + record.stepId(),
                e);
        }
    }

    private ValidatedCompletion completionContract(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command
    ) {
        AwaitStepDescriptor descriptor = descriptorFor(record);
        validateDurableOutputContract(record, descriptor);
        Object completionPayload = sameTypeIdentity(record.outputType(), record.transportOutputType())
            ? command.responsePayload()
            : canonicalCompletionPayload(record, descriptor, command.responsePayload());
        return new ValidatedCompletion(record, withResponsePayload(command, completionPayload), descriptor);
    }

    /**
     * Admits a transport completion at the await boundary and converts it before it crosses the
     * durable canonical boundary. New interaction records therefore persist canonical values;
     * the legacy transport projection remains readable only when restoring older records.
     */
    private Object canonicalCompletionPayload(
        AwaitInteractionRecord record,
        AwaitStepDescriptor descriptor,
        Object payload
    ) {
        Object transportPayload = coerceTransportPayload(record, payload);
        Object canonicalPayload = descriptor.outputFromTransport().apply(transportPayload);
        return coerceCanonicalPayload(record, canonicalPayload);
    }

    private Object coerceTransportPayload(AwaitInteractionRecord record, Object payload) {
        try {
            Class<?> transportOutputType = AwaitPayloadSupport.resolvePayloadClass(
                record.transportOutputType(),
                Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(payload, transportOutputType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Await transport completion validation failed for interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": transportOutputType " + record.transportOutputType() + " is unavailable",
                e);
        }
    }

    private Object coerceCanonicalPayload(AwaitInteractionRecord record, Object payload) {
        try {
            Class<?> canonicalOutputType = AwaitPayloadSupport.resolvePayloadClass(
                record.outputType(),
                Thread.currentThread().getContextClassLoader());
            return AwaitPayloadSupport.coercePayload(payload, canonicalOutputType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Await canonical completion validation failed for interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": outputType " + record.outputType() + " is unavailable",
                e);
        }
    }

    private void validateDurableOutputContract(AwaitInteractionRecord record, AwaitStepDescriptor descriptor) {
        if (!sameTypeIdentity(descriptor.outputType(), record.outputType())) {
            throw new IllegalStateException(
                "Await durable-contract compatibility failed for execution " + record.executionId()
                    + " interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": durable canonical outputType=" + record.outputType()
                    + " differs from rebuilt descriptor outputType=" + descriptor.outputType());
        }
        if (!sameTypeIdentity(descriptor.transportOutputType(), record.transportOutputType())) {
            throw new IllegalStateException(
                "Await durable transport-contract compatibility failed for execution " + record.executionId()
                    + " interaction " + record.interactionId()
                    + " stepId=" + record.stepId()
                    + ": durable transportOutputType=" + record.transportOutputType()
                    + " differs from rebuilt descriptor transportOutputType=" + descriptor.transportOutputType());
        }
    }

    /**
     * Compares durable type identities by their loadable class when source-form names differ from
     * JVM binary names for nested types. This preserves v2's single protobuf identity while still
     * rejecting a v3 protobuf transport type in place of its canonical domain contract.
     */
    private static boolean sameTypeIdentity(String left, String right) {
        if (left.equals(right)) {
            return true;
        }
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            return AwaitPayloadSupport.resolvePayloadClass(left, classLoader)
                .equals(AwaitPayloadSupport.resolvePayloadClass(right, classLoader));
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    private Uni<Void> registerDescriptor(AwaitStepDescriptor descriptor) {
        return Uni.createFrom().item(() -> {
            directDescriptors.putIfAbsent(descriptor.stepId(), descriptor);
            if (descriptorFactory != null) {
                descriptorFactory.register(descriptor);
            }
            return descriptor;
        }).replaceWithVoid();
    }

    private AwaitStepDescriptor descriptorFor(AwaitInteractionRecord record) {
        try {
            if (descriptorFactory != null) {
                return descriptorFactory.descriptorByStepIdNow(record.stepId());
            }
            AwaitStepDescriptor descriptor = directDescriptors.get(record.stepId());
            if (descriptor != null) {
                return descriptor;
            }
        } catch (RuntimeException e) {
            throw durableDescriptorFailure(record, e);
        }
        throw durableDescriptorFailure(record, null);
    }

    private static IllegalStateException durableDescriptorFailure(
        AwaitInteractionRecord record,
        Throwable cause
    ) {
        String message = "Await durable-contract resolution failed for execution " + record.executionId()
            + " interaction " + record.interactionId()
            + " stepId=" + record.stepId()
            + ": no AwaitStepDescriptor is available";
        return cause == null ? new IllegalStateException(message) : new IllegalStateException(message, cause);
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

    private record ValidatedCompletion(
        AwaitInteractionRecord record,
        AwaitCompletionCommand command,
        AwaitStepDescriptor descriptor
    ) {
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
