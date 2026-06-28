package org.pipelineframework.orchestrator.controlplane;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

/**
 * Compatibility bridge from existing queue-async projection writes to immutable segment/boundary facts.
 */
@ApplicationScoped
public class SegmentBoundaryLedger {

    private static final Logger LOG = Logger.getLogger(SegmentBoundaryLedger.class);
    private static final int MAX_APPEND_ATTEMPTS = 3;

    @Inject
    Instance<ControlPlaneJournal> journals;

    private final ControlPlaneJournal explicitJournal;

    public SegmentBoundaryLedger() {
        this(null);
    }

    public SegmentBoundaryLedger(ControlPlaneJournal journal) {
        this.explicitJournal = journal;
    }

    public Uni<Void> recordRunSubmitted(
        CreateExecutionResult created,
        ExecutionCreateCommand command,
        long nowEpochMs
    ) {
        if (created == null || created.duplicate() || created.record() == null || command == null) {
            return Uni.createFrom().voidItem();
        }
        ExecutionRecord<Object, Object> record = created.record();
        return appendBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.RunSubmitted(
                record.tenantId(),
                record.executionId(),
                record.executionKey(),
                record.pipelineId(),
                record.contractVersion(),
                record.releaseVersion(),
                record.resultShape(),
                segmentId(record.executionId(), record.currentStepIndex()),
                record.currentStepIndex(),
                -1,
                command.inputPayload(),
                record.ttlEpochS())),
            nowEpochMs);
    }

    public Uni<Void> recordSegmentAttemptStarted(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.SegmentAttemptStarted(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                attemptId(record, transitionKey),
                record.attempt())),
            nowEpochMs);
    }

    public Uni<Void> recordSegmentCompleted(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionResultEnvelope result,
        long nowEpochMs
    ) {
        if (record == null || result == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBestEffortBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.SegmentCompleted(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                attemptId(record, transitionKey),
                result.coordinatorOutputItems(),
                true)),
            nowEpochMs);
    }

    public Uni<Void> recordSegmentSuspended(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionAwaitSuspension suspension,
        long nowEpochMs
    ) {
        if (record == null || suspension == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(record.tenantId(), record.executionId(), () -> {
            List<ControlPlaneFact> facts = new ArrayList<>();
            AwaitUnitRecord unit = suspension.unit();
            facts.add(new ControlPlaneFact.SegmentSuspended(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                attemptId(record, transitionKey),
                suspension.unitId(),
                BoundaryKind.AWAIT,
                suspension.stepIndex(),
                unit == null ? null : unit.expectedItemCount()));
            for (AwaitInteractionRecord interaction : suspension.interactions()) {
                facts.add(boundaryInteractionDispatched(record.executionId(), interaction));
            }
            if (unit != null && unit.dispatchComplete() && unit.expectedItemCount() != null) {
                facts.add(new ControlPlaneFact.BoundaryDispatchCompleted(
                    unit.tenantId(),
                    unit.executionId(),
                    unit.unitId(),
                    unit.expectedItemCount()));
            }
            return facts;
        }, nowEpochMs);
    }

    public Uni<Void> recordBoundaryCompletionAdmitted(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        long nowEpochMs
    ) {
        if (record == null || unit == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(BoundaryAdmissionFacts.completion(new BoundaryAdmissionRequest(
                record.tenantId(),
                record.executionId(),
                unit.unitId(),
                BoundaryKind.AWAIT,
                record.interactionId(),
                completionIdempotencyKey(record),
                record.responsePayload()))),
            nowEpochMs);
    }

    public Uni<Void> recordInteractionTimedOut(AwaitInteractionRecord record, long nowEpochMs) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.InteractionTimedOut(
                record.tenantId(),
                record.executionId(),
                record.unitId(),
                record.interactionId(),
                "Await interaction timed out: " + record.interactionId())),
            nowEpochMs);
    }

    public Uni<Void> recordContinuationSegmentCreated(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        String segmentId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload,
        long nowEpochMs
    ) {
        if (parent == null || unit == null || segmentId == null || segmentId.isBlank()) {
            return Uni.createFrom().voidItem();
        }
        return recordContinuationSegmentCreated(
            parent,
            unit.unitId(),
            segmentId,
            startStepIndex,
            stopBeforeStepIndex,
            inputPayload,
            nowEpochMs);
    }

    public Uni<Void> recordContinuationSegmentCreated(
        ExecutionRecord<Object, Object> parent,
        String boundaryUnitId,
        String segmentId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload,
        long nowEpochMs
    ) {
        if (parent == null || boundaryUnitId == null || boundaryUnitId.isBlank()
            || segmentId == null || segmentId.isBlank()) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(
            parent.tenantId(),
            parent.executionId(),
            () -> List.of(new ControlPlaneFact.ContinuationSegmentCreated(
                parent.tenantId(),
                parent.executionId(),
                segmentId(parent),
                segmentId,
                boundaryUnitId,
                startStepIndex,
                stopBeforeStepIndex,
                inputPayload)),
            nowEpochMs);
    }

    public Uni<Void> recordContinuationSegmentCreated(
        String tenantId,
        String runId,
        String sourceSegmentId,
        String boundaryUnitId,
        String segmentId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload,
        long nowEpochMs
    ) {
        if (tenantId == null || tenantId.isBlank()
            || runId == null || runId.isBlank()
            || sourceSegmentId == null || sourceSegmentId.isBlank()
            || boundaryUnitId == null || boundaryUnitId.isBlank()
            || segmentId == null || segmentId.isBlank()) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(
            tenantId,
            runId,
            () -> List.of(new ControlPlaneFact.ContinuationSegmentCreated(
                tenantId,
                runId,
                sourceSegmentId,
                segmentId,
                boundaryUnitId,
                startStepIndex,
                stopBeforeStepIndex,
                inputPayload)),
            nowEpochMs);
    }

    public Uni<Void> recordTerminalPublicationCompleted(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        String idempotencyKey = attemptId(record, transitionKey) + ":terminal-publication";
        return appendBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.TerminalPublicationCompleted(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                record.executionId() + ":terminal-output",
                idempotencyKey)),
            nowEpochMs);
    }

    public Uni<Void> recordRunSucceeded(
        ExecutionRecord<Object, Object> record,
        Object resultPayload,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBestEffortBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.RunSucceeded(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                resultPayload)),
            nowEpochMs);
    }

    public Uni<Void> recordRunFailed(
        ExecutionRecord<Object, Object> record,
        String errorCode,
        String errorMessage,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        return appendBuilt(
            record.tenantId(),
            record.executionId(),
            () -> List.of(new ControlPlaneFact.RunFailed(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                errorCode == null || errorCode.isBlank() ? "EXECUTION_FAILED" : errorCode,
                errorMessage == null ? "" : errorMessage)),
            nowEpochMs);
    }

    public static String segmentId(ExecutionRecord<Object, Object> record) {
        return segmentId(record.executionId(), record.currentStepIndex());
    }

    public static String segmentId(String executionId, int stepIndex) {
        return executionId + ":segment:" + stepIndex;
    }

    public static String itemContinuationSegmentId(String executionId, String unitId, int itemIndex) {
        return executionId + ":segment:await-item:" + unitId + ":" + itemIndex;
    }

    private Uni<Void> append(
        String tenantId,
        String runId,
        List<ControlPlaneFact> facts,
        long nowEpochMs
    ) {
        Optional<ControlPlaneJournal> journal = journal();
        if (journal.isEmpty() || facts == null || facts.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return appendWithRetry(journal.get(), tenantId, runId, List.copyOf(facts), nowEpochMs, 1)
            .replaceWithVoid()
            .onFailure().recoverWithUni(failure -> {
                LOG.warnf(
                    failure,
                    "Failed appending queue-async control-plane facts tenant=%s runId=%s",
                    tenantId,
                    runId);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> appendBuilt(
        String tenantId,
        String runId,
        Supplier<List<ControlPlaneFact>> facts,
        long nowEpochMs
    ) {
        return append(tenantId, runId, facts.get(), nowEpochMs);
    }

    private Uni<Void> appendBestEffortBuilt(
        String tenantId,
        String runId,
        Supplier<List<ControlPlaneFact>> facts,
        long nowEpochMs
    ) {
        try {
            return append(tenantId, runId, facts.get(), nowEpochMs);
        } catch (IllegalArgumentException failure) {
            LOG.warnf(
                failure,
                "Failed building queue-async control-plane facts tenant=%s runId=%s",
                tenantId,
                runId);
            return Uni.createFrom().voidItem();
        }
    }

    private Uni<ControlPlaneAppendResult> appendWithRetry(
        ControlPlaneJournal journal,
        String tenantId,
        String runId,
        List<ControlPlaneFact> facts,
        long nowEpochMs,
        int attempt
    ) {
        return journal.projection(tenantId, runId)
            .onItem().transformToUni(projection -> {
                List<ControlPlaneFact> missingFacts = facts.stream()
                    .filter(fact -> !projection.factKeys().contains(fact.factKey()))
                    .toList();
                if (missingFacts.isEmpty()) {
                    return Uni.createFrom().item(new ControlPlaneAppendResult(projection, List.of()));
                }
                return journal.append(tenantId, runId, projection.version(), missingFacts, nowEpochMs);
            })
            .onFailure(ControlPlaneAppendConflictException.class).recoverWithUni(failure -> {
                if (attempt >= MAX_APPEND_ATTEMPTS) {
                    return Uni.createFrom().failure(failure);
                }
                return appendWithRetry(journal, tenantId, runId, facts, nowEpochMs, attempt + 1);
            });
    }

    private Optional<ControlPlaneJournal> journal() {
        if (explicitJournal != null) {
            return Optional.of(explicitJournal);
        }
        if (journals == null || journals.isUnsatisfied()) {
            return Optional.empty();
        }
        return Optional.of(journals.get());
    }

    private static ControlPlaneFact.BoundaryInteractionDispatched boundaryInteractionDispatched(
        String runId,
        AwaitInteractionRecord interaction
    ) {
        return new ControlPlaneFact.BoundaryInteractionDispatched(
            interaction.tenantId(),
            runId,
            interaction.unitId(),
            BoundaryKind.AWAIT,
            interaction.interactionId(),
            interaction.correlationId(),
            interaction.idempotencyKey(),
            interaction.itemIndex(),
            interaction.requestPayload(),
            interaction.transportType(),
            interaction.deadlineEpochMs());
    }

    private static String completionIdempotencyKey(AwaitInteractionRecord record) {
        if (record.itemIndex() != null) {
            return "item:" + record.itemIndex();
        }
        return record.idempotencyKey();
    }

    private static String attemptId(ExecutionRecord<Object, Object> record, String transitionKey) {
        if (transitionKey != null && !transitionKey.isBlank()) {
            return transitionKey;
        }
        return record.executionId() + ":" + record.currentStepIndex() + ":" + record.attempt();
    }
}
