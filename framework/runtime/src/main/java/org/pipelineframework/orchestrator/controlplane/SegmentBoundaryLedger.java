package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

/**
 * Compatibility bridge from existing queue-async projection writes to immutable segment/boundary facts.
 */
@ApplicationScoped
public class SegmentBoundaryLedger {

    @Inject
    Instance<ControlPlaneJournal> journals;

    private final ControlPlaneJournal explicitJournal;
    private final SegmentBoundaryFactFactory facts;
    private final SegmentBoundaryJournalAppender appender;
    private final TerminalPublicationLedger terminalPublications;

    public SegmentBoundaryLedger() {
        this(null);
    }

    public SegmentBoundaryLedger(ControlPlaneJournal journal) {
        this.explicitJournal = journal;
        this.facts = new SegmentBoundaryFactFactory();
        this.appender = new SegmentBoundaryJournalAppender(this::journal);
        this.terminalPublications = new TerminalPublicationLedger(this::journal, appender, facts);
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
        return appender.appendFactsOrThrowBuildFailure(
            record.tenantId(),
            record.executionId(),
            () -> facts.runSubmitted(created, command),
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
        return appender.appendFactsOrThrowBuildFailure(
            record.tenantId(),
            record.executionId(),
            () -> facts.segmentAttemptStarted(record, transitionKey),
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
        return appender.appendFactsBestEffort(
            record.tenantId(),
            record.executionId(),
            () -> facts.segmentCompleted(record, transitionKey, result),
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
        return appender.appendFactsOrThrowBuildFailure(
            record.tenantId(),
            record.executionId(),
            () -> facts.segmentSuspended(record, transitionKey, suspension),
            nowEpochMs);
    }

    public Uni<Void> recordBoundaryCompletionAdmitted(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        long nowEpochMs
    ) {
        if (record == null || unit == null) {
            return Uni.createFrom().voidItem();
        }
        return appender.appendFactsOrThrowBuildFailure(
            record.tenantId(),
            record.executionId(),
            () -> facts.boundaryCompletionAdmitted(record, unit),
            nowEpochMs);
    }

    public Uni<Void> recordInteractionTimedOut(AwaitInteractionRecord record, long nowEpochMs) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        return appender.appendFactsOrThrowBuildFailure(
            record.tenantId(),
            record.executionId(),
            () -> facts.interactionTimedOut(record),
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
        return appender.appendFactsOrThrowBuildFailure(
            parent.tenantId(),
            parent.executionId(),
            () -> facts.continuationSegmentCreated(
                parent,
                boundaryUnitId,
                segmentId,
                startStepIndex,
                stopBeforeStepIndex,
                inputPayload),
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
        return appender.appendFactsOrThrowBuildFailure(
            tenantId,
            runId,
            () -> facts.continuationSegmentCreated(
                tenantId,
                runId,
                sourceSegmentId,
                boundaryUnitId,
                segmentId,
                startStepIndex,
                stopBeforeStepIndex,
                inputPayload),
            nowEpochMs);
    }

    public Uni<Void> recordTerminalPublicationCompleted(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        long nowEpochMs
    ) {
        return completeTerminalPublication(record, transitionKey, "object-publish", nowEpochMs);
    }

    public Uni<TerminalPublicationClaim> claimTerminalPublication(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        String publicationKind,
        long nowEpochMs
    ) {
        return terminalPublications.claim(record, transitionKey, publicationKind, nowEpochMs);
    }

    public Uni<Void> completeTerminalPublication(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        String publicationKind,
        long nowEpochMs
    ) {
        return terminalPublications.complete(record, transitionKey, publicationKind, nowEpochMs);
    }

    public Uni<Void> recordRunSucceeded(
        ExecutionRecord<Object, Object> record,
        Object resultPayload,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        return appender.appendFactsBestEffort(
            record.tenantId(),
            record.executionId(),
            () -> facts.runSucceeded(record, resultPayload),
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
        return appender.appendFactsOrThrowBuildFailure(
            record.tenantId(),
            record.executionId(),
            () -> facts.runFailed(record, errorCode, errorMessage),
            nowEpochMs);
    }

    public static String segmentId(ExecutionRecord<Object, Object> record) {
        return SegmentBoundaryFactFactory.segmentId(record);
    }

    public static String segmentId(String executionId, int stepIndex) {
        return SegmentBoundaryFactFactory.segmentId(executionId, stepIndex);
    }

    public static String itemContinuationSegmentId(String executionId, String unitId, int itemIndex) {
        return SegmentBoundaryFactFactory.itemContinuationSegmentId(executionId, unitId, itemIndex);
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
}
