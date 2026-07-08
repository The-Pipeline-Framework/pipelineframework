package org.pipelineframework.orchestrator.controlplane;

import java.util.ArrayList;
import java.util.List;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

final class SegmentBoundaryFactFactory {

    List<ControlPlaneFact> runSubmitted(CreateExecutionResult created, ExecutionCreateCommand command) {
        ExecutionRecord<Object, Object> record = created.record();
        return List.of(new ControlPlaneFact.RunSubmitted(
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
            record.ttlEpochS()));
    }

    List<ControlPlaneFact> segmentAttemptStarted(
        ExecutionRecord<Object, Object> record,
        String transitionKey
    ) {
        return List.of(new ControlPlaneFact.SegmentAttemptStarted(
            record.tenantId(),
            record.executionId(),
            segmentId(record),
            attemptId(record, transitionKey),
            record.attempt()));
    }

    List<ControlPlaneFact> segmentCompleted(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionResultEnvelope result
    ) {
        return List.of(new ControlPlaneFact.SegmentCompleted(
            record.tenantId(),
            record.executionId(),
            segmentId(record),
            attemptId(record, transitionKey),
            result.coordinatorOutputItems(),
            true));
    }

    List<ControlPlaneFact> segmentSuspended(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionAwaitSuspension suspension
    ) {
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
    }

    List<ControlPlaneFact> boundaryCompletionAdmitted(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit
    ) {
        return List.of(BoundaryAdmissionFacts.completion(new BoundaryAdmissionRequest(
            record.tenantId(),
            record.executionId(),
            unit.unitId(),
            BoundaryKind.AWAIT,
            record.interactionId(),
            completionIdempotencyKey(record),
            record.responsePayload())));
    }

    List<ControlPlaneFact> interactionTimedOut(AwaitInteractionRecord record) {
        return List.of(new ControlPlaneFact.InteractionTimedOut(
            record.tenantId(),
            record.executionId(),
            record.unitId(),
            record.interactionId(),
            "Await interaction timed out: " + record.interactionId()));
    }

    List<ControlPlaneFact> continuationSegmentCreated(
        ExecutionRecord<Object, Object> parent,
        String boundaryUnitId,
        String segmentId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload
    ) {
        return continuationSegmentCreated(
            parent.tenantId(),
            parent.executionId(),
            segmentId(parent),
            boundaryUnitId,
            segmentId,
            startStepIndex,
            stopBeforeStepIndex,
            inputPayload);
    }

    List<ControlPlaneFact> continuationSegmentCreated(
        String tenantId,
        String runId,
        String sourceSegmentId,
        String boundaryUnitId,
        String segmentId,
        int startStepIndex,
        int stopBeforeStepIndex,
        Object inputPayload
    ) {
        return List.of(new ControlPlaneFact.ContinuationSegmentCreated(
            tenantId,
            runId,
            sourceSegmentId,
            segmentId,
            boundaryUnitId,
            startStepIndex,
            stopBeforeStepIndex,
            inputPayload));
    }

    List<ControlPlaneFact> runSucceeded(
        ExecutionRecord<Object, Object> record,
        Object resultPayload
    ) {
        return List.of(new ControlPlaneFact.RunSucceeded(
            record.tenantId(),
            record.executionId(),
            segmentId(record),
            resultPayload));
    }

    List<ControlPlaneFact> runFailed(
        ExecutionRecord<Object, Object> record,
        String errorCode,
        String errorMessage
    ) {
        return List.of(new ControlPlaneFact.RunFailed(
            record.tenantId(),
            record.executionId(),
            segmentId(record),
            errorCode == null || errorCode.isBlank() ? "EXECUTION_FAILED" : errorCode,
            errorMessage == null ? "" : errorMessage));
    }

    SegmentTerminalPublicationFacts terminalPublicationFacts(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        String publicationKind
    ) {
        String publicationId = record.executionId() + ":terminal-output:" + publicationKind(publicationKind);
        String idempotencyKey = publicationId + ":terminal-publication";
        return new SegmentTerminalPublicationFacts(
            publicationId,
            idempotencyKey,
            new ControlPlaneFact.TerminalPublicationPrepared(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                publicationId,
                idempotencyKey),
            new ControlPlaneFact.TerminalPublicationCompleted(
                record.tenantId(),
                record.executionId(),
                segmentId(record),
                publicationId,
                idempotencyKey));
    }

    static String segmentId(ExecutionRecord<Object, Object> record) {
        return segmentId(record.executionId(), record.currentStepIndex());
    }

    static String segmentId(String executionId, int stepIndex) {
        return executionId + ":segment:" + stepIndex;
    }

    static String itemContinuationSegmentId(String executionId, String unitId, int itemIndex) {
        return executionId + ":segment:await-item:" + unitId + ":" + itemIndex;
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

    private static String publicationKind(String publicationKind) {
        if (publicationKind == null || publicationKind.isBlank()) {
            throw new IllegalArgumentException("publicationKind must not be blank");
        }
        return publicationKind.trim();
    }
}
