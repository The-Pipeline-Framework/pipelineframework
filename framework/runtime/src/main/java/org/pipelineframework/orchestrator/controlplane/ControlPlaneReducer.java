package org.pipelineframework.orchestrator.controlplane;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class ControlPlaneReducer {

    private ControlPlaneReducer() {
    }

    public static ControlPlaneProjection reduce(String tenantId, String runId, List<ControlPlaneEvent> events) {
        Accumulator accumulator = new Accumulator(tenantId, runId);
        ControlPlaneChecks.copyList(events).stream()
            .sorted(Comparator.comparingLong(ControlPlaneEvent::sequence))
            .forEach(accumulator::apply);
        return accumulator.projection();
    }

    private static final class Accumulator {
        private final String tenantId;
        private final String runId;
        private long version;
        private PipelineRun run;
        private final Map<String, ExecutionSegment> segments = new HashMap<>();
        private final Map<String, SegmentAttempt> attempts = new HashMap<>();
        private final Map<String, BoundaryUnit> boundaries = new HashMap<>();
        private final Map<String, BoundaryInteraction> interactions = new HashMap<>();
        private final Set<String> terminalPublicationKeys = new HashSet<>();
        private final Set<String> factKeys = new HashSet<>();

        private Accumulator(String tenantId, String runId) {
            this.tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            this.runId = ControlPlaneChecks.requireText(runId, "runId");
        }

        private void apply(ControlPlaneEvent event) {
            if (!tenantId.equals(event.fact().tenantId()) || !runId.equals(event.fact().runId())) {
                throw new IllegalArgumentException("event does not belong to projection " + tenantId + "/" + runId);
            }
            version = Math.max(version, event.sequence());
            factKeys.add(event.fact().factKey());
            switch (event.fact()) {
                case ControlPlaneFact.RunSubmitted fact -> applyRunSubmitted(event, fact);
                case ControlPlaneFact.SegmentAttemptStarted fact -> applySegmentAttemptStarted(event, fact);
                case ControlPlaneFact.SegmentCompleted fact -> applySegmentCompleted(event, fact);
                case ControlPlaneFact.SegmentSuspended fact -> applySegmentSuspended(event, fact);
                case ControlPlaneFact.BoundaryInteractionDispatched fact -> applyBoundaryInteractionDispatched(event, fact);
                case ControlPlaneFact.BoundaryDispatchCompleted fact -> applyBoundaryDispatchCompleted(event, fact);
                case ControlPlaneFact.BoundaryCompletionAdmitted fact -> applyBoundaryCompletionAdmitted(event, fact);
                case ControlPlaneFact.InteractionTimedOut fact -> applyInteractionTimedOut(event, fact);
                case ControlPlaneFact.ContinuationSegmentCreated fact -> applyContinuationSegmentCreated(event, fact);
                case ControlPlaneFact.TerminalPublicationCompleted fact -> terminalPublicationKeys.add(fact.factKey());
                case ControlPlaneFact.RunSucceeded fact -> applyRunSucceeded(event, fact);
                case ControlPlaneFact.RunFailed fact -> applyRunFailed(event, fact);
            }
        }

        private void applyRunSubmitted(ControlPlaneEvent event, ControlPlaneFact.RunSubmitted fact) {
            run = new PipelineRun(
                fact.tenantId(),
                fact.runId(),
                fact.executionKey(),
                fact.pipelineId(),
                fact.contractVersion(),
                fact.releaseVersion(),
                fact.resultShape(),
                PipelineRunStatus.ACCEPTED,
                fact.inputPayload(),
                null,
                null,
                null,
                event.sequence(),
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs(),
                fact.ttlEpochS());
            segments.put(fact.initialSegmentId(), new ExecutionSegment(
                fact.tenantId(),
                fact.runId(),
                fact.initialSegmentId(),
                fact.startStepIndex(),
                fact.stopBeforeStepIndex(),
                SegmentStatus.QUEUED,
                fact.inputPayload(),
                List.of(),
                null,
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs()));
        }

        private void applySegmentAttemptStarted(ControlPlaneEvent event, ControlPlaneFact.SegmentAttemptStarted fact) {
            ExecutionSegment segment = segments.get(fact.segmentId());
            if (segment != null) {
                segments.put(fact.segmentId(), segment.running(event.occurredAtEpochMs()));
            }
            attempts.put(fact.attemptId(), new SegmentAttempt(
                fact.tenantId(),
                fact.runId(),
                fact.segmentId(),
                fact.attemptId(),
                fact.attemptNumber(),
                SegmentAttemptStatus.RUNNING,
                null,
                null,
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs()));
            if (run != null && !run.status().terminal()) {
                run = run.withStatus(PipelineRunStatus.RUNNING, event.sequence(), event.occurredAtEpochMs());
            }
        }

        private void applySegmentCompleted(ControlPlaneEvent event, ControlPlaneFact.SegmentCompleted fact) {
            ExecutionSegment segment = segments.get(fact.segmentId());
            if (segment != null) {
                segments.put(fact.segmentId(), segment.completed(fact.outputItems(), event.occurredAtEpochMs()));
            }
            SegmentAttempt attempt = attempts.get(fact.attemptId());
            if (attempt != null) {
                attempts.put(fact.attemptId(), attempt.withStatus(SegmentAttemptStatus.COMPLETED, event.occurredAtEpochMs()));
            }
        }

        private void applySegmentSuspended(ControlPlaneEvent event, ControlPlaneFact.SegmentSuspended fact) {
            ExecutionSegment segment = segments.get(fact.segmentId());
            if (segment != null) {
                segments.put(fact.segmentId(), segment.suspended(fact.boundaryUnitId(), event.occurredAtEpochMs()));
            }
            SegmentAttempt attempt = attempts.get(fact.attemptId());
            if (attempt != null) {
                attempts.put(fact.attemptId(), attempt.withStatus(SegmentAttemptStatus.SUSPENDED, event.occurredAtEpochMs()));
            }
            boundaries.put(fact.boundaryUnitId(), new BoundaryUnit(
                fact.tenantId(),
                fact.runId(),
                fact.boundaryUnitId(),
                fact.boundaryKind(),
                BoundaryUnitStatus.OPEN,
                fact.segmentId(),
                fact.attemptId(),
                fact.boundaryStepIndex(),
                fact.expectedItemCount(),
                0,
                Set.of(),
                false,
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs()));
            if (run != null && !run.status().terminal()) {
                run = run.withStatus(PipelineRunStatus.WAITING_BOUNDARY, event.sequence(), event.occurredAtEpochMs());
            }
        }

        private void applyBoundaryInteractionDispatched(
            ControlPlaneEvent event,
            ControlPlaneFact.BoundaryInteractionDispatched fact
        ) {
            interactions.put(fact.interactionId(), new BoundaryInteraction(
                fact.tenantId(),
                fact.runId(),
                fact.boundaryUnitId(),
                fact.interactionId(),
                fact.boundaryKind(),
                BoundaryInteractionStatus.DISPATCHED,
                fact.correlationId(),
                fact.idempotencyKey(),
                fact.itemIndex(),
                fact.requestPayload(),
                null,
                fact.transportType(),
                fact.deadlineEpochMs(),
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs()));
        }

        private void applyBoundaryDispatchCompleted(ControlPlaneEvent event, ControlPlaneFact.BoundaryDispatchCompleted fact) {
            BoundaryUnit unit = boundaries.get(fact.boundaryUnitId());
            if (unit != null) {
                boundaries.put(fact.boundaryUnitId(), unit.withDispatchComplete(
                    fact.expectedItemCount(),
                    event.occurredAtEpochMs()));
            }
        }

        private void applyBoundaryCompletionAdmitted(
            ControlPlaneEvent event,
            ControlPlaneFact.BoundaryCompletionAdmitted fact
        ) {
            BoundaryInteraction interaction = interactions.get(fact.interactionId());
            if (interaction != null && !interaction.status().terminal()) {
                interactions.put(fact.interactionId(), interaction.completed(
                    fact.responsePayload(),
                    event.occurredAtEpochMs()));
            }
            BoundaryUnit unit = boundaries.get(fact.boundaryUnitId());
            if (unit != null && !unit.status().terminal()) {
                boundaries.put(fact.boundaryUnitId(), unit.withCompletion(
                    fact.idempotencyKey(),
                    event.occurredAtEpochMs()));
            }
        }

        private void applyInteractionTimedOut(ControlPlaneEvent event, ControlPlaneFact.InteractionTimedOut fact) {
            BoundaryInteraction interaction = interactions.get(fact.interactionId());
            if (interaction != null && !interaction.status().terminal()) {
                interactions.put(fact.interactionId(), interaction.timedOut(event.occurredAtEpochMs()));
            }
            BoundaryUnit unit = boundaries.get(fact.boundaryUnitId());
            if (unit != null && !unit.status().terminal()) {
                boundaries.put(fact.boundaryUnitId(), unit.timedOut(event.occurredAtEpochMs()));
                ExecutionSegment segment = segments.get(unit.segmentId());
                if (segment != null) {
                    segments.put(segment.segmentId(), segment.failed(event.occurredAtEpochMs()));
                }
                SegmentAttempt attempt = attempts.get(unit.attemptId());
                if (attempt != null) {
                    attempts.put(unit.attemptId(), attempt.failed(
                        "BOUNDARY_TIMEOUT",
                        fact.reason(),
                        event.occurredAtEpochMs()));
                }
            }
            if (run != null && !run.status().terminal()) {
                run = run.failed("BOUNDARY_TIMEOUT", fact.reason(), event.sequence(), event.occurredAtEpochMs());
            }
        }

        private void applyContinuationSegmentCreated(
            ControlPlaneEvent event,
            ControlPlaneFact.ContinuationSegmentCreated fact
        ) {
            segments.put(fact.segmentId(), new ExecutionSegment(
                fact.tenantId(),
                fact.runId(),
                fact.segmentId(),
                fact.startStepIndex(),
                fact.stopBeforeStepIndex(),
                SegmentStatus.QUEUED,
                fact.inputPayload(),
                List.of(),
                fact.boundaryUnitId(),
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs(),
                event.occurredAtEpochMs()));
            if (run != null && !run.status().terminal()) {
                run = run.withStatus(PipelineRunStatus.RUNNING, event.sequence(), event.occurredAtEpochMs());
            }
        }

        private void applyRunSucceeded(ControlPlaneEvent event, ControlPlaneFact.RunSucceeded fact) {
            if (run != null && !run.status().terminal()) {
                run = run.succeeded(fact.resultPayload(), event.sequence(), event.occurredAtEpochMs());
            }
        }

        private void applyRunFailed(ControlPlaneEvent event, ControlPlaneFact.RunFailed fact) {
            if (run != null && !run.status().terminal()) {
                run = run.failed(fact.errorCode(), fact.errorMessage(), event.sequence(), event.occurredAtEpochMs());
            }
        }

        private ControlPlaneProjection projection() {
            return new ControlPlaneProjection(
                tenantId,
                runId,
                version,
                Optional.ofNullable(run),
                new HashMap<>(segments),
                new HashMap<>(attempts),
                new HashMap<>(boundaries),
                new HashMap<>(interactions),
                new HashSet<>(terminalPublicationKeys),
                new HashSet<>(factKeys));
        }
    }
}
