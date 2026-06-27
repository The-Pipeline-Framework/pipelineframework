package org.pipelineframework.orchestrator.controlplane;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import io.smallrye.mutiny.Uni;

public class InMemoryControlPlaneJournal implements ControlPlaneJournal {

    private final Object lock = new Object();
    private final Map<Key, List<ControlPlaneEvent>> eventsByRun = new HashMap<>();

    @Override
    public Uni<ControlPlaneAppendResult> append(
        String tenantId,
        String runId,
        long expectedVersion,
        List<ControlPlaneFact> facts,
        long nowEpochMs
    ) {
        return Uni.createFrom().item(() -> appendBlocking(tenantId, runId, expectedVersion, facts, nowEpochMs));
    }

    @Override
    public Uni<ControlPlaneProjection> projection(String tenantId, String runId) {
        return Uni.createFrom().item(() -> projectionBlocking(tenantId, runId));
    }

    @Override
    public Uni<List<DueSegment>> findDueSegments(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> findDueSegmentsBlocking(nowEpochMs, limit));
    }

    @Override
    public Uni<List<DueBoundaryInteraction>> findTimedOutInteractions(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> findTimedOutInteractionsBlocking(nowEpochMs, limit));
    }

    private ControlPlaneAppendResult appendBlocking(
        String tenantId,
        String runId,
        long expectedVersion,
        List<ControlPlaneFact> facts,
        long nowEpochMs
    ) {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        ControlPlaneChecks.requireNonNegative(expectedVersion, "expectedVersion");
        ControlPlaneChecks.requireNonNegative(nowEpochMs, "nowEpochMs");
        List<ControlPlaneFact> requestedFacts = ControlPlaneChecks.copyList(facts);
        if (requestedFacts.isEmpty()) {
            return new ControlPlaneAppendResult(projectionBlocking(tenantId, runId), List.of());
        }
        for (ControlPlaneFact fact : requestedFacts) {
            if (!tenantId.equals(fact.tenantId()) || !runId.equals(fact.runId())) {
                throw new IllegalArgumentException("fact does not belong to append target " + tenantId + "/" + runId);
            }
        }

        synchronized (lock) {
            Key key = new Key(tenantId, runId);
            List<ControlPlaneEvent> currentEvents = eventsByRun.computeIfAbsent(key, ignored -> new ArrayList<>());
            ControlPlaneProjection current = ControlPlaneReducer.reduce(tenantId, runId, currentEvents);
            Set<String> knownFactKeys = new HashSet<>(current.factKeys());
            List<ControlPlaneFact> newFacts = new ArrayList<>();
            for (ControlPlaneFact fact : requestedFacts) {
                if (knownFactKeys.add(fact.factKey())) {
                    newFacts.add(fact);
                }
            }
            if (newFacts.isEmpty()) {
                return new ControlPlaneAppendResult(current, List.of());
            }
            if (current.version() != expectedVersion) {
                throw new ControlPlaneAppendConflictException(
                    "Control-plane append expected version " + expectedVersion
                        + " but current version is " + current.version()
                        + " for run " + tenantId + "/" + runId);
            }

            long sequence = current.version();
            List<ControlPlaneEvent> appended = new ArrayList<>();
            for (ControlPlaneFact fact : newFacts) {
                ControlPlaneEvent event = new ControlPlaneEvent(++sequence, nowEpochMs, fact);
                currentEvents.add(event);
                appended.add(event);
            }
            ControlPlaneProjection updated = ControlPlaneReducer.reduce(tenantId, runId, currentEvents);
            return new ControlPlaneAppendResult(updated, appended);
        }
    }

    private ControlPlaneProjection projectionBlocking(String tenantId, String runId) {
        tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
        runId = ControlPlaneChecks.requireText(runId, "runId");
        synchronized (lock) {
            List<ControlPlaneEvent> events = eventsByRun.getOrDefault(new Key(tenantId, runId), List.of());
            return ControlPlaneReducer.reduce(tenantId, runId, events);
        }
    }

    private List<DueSegment> findDueSegmentsBlocking(long nowEpochMs, int limit) {
        ControlPlaneChecks.requireNonNegative(nowEpochMs, "nowEpochMs");
        if (limit <= 0) {
            return List.of();
        }
        synchronized (lock) {
            return projections().stream()
                .flatMap(projection -> projection.segments().values().stream())
                .filter(segment -> segment.status() == SegmentStatus.QUEUED)
                .filter(segment -> segment.nextDueEpochMs() <= nowEpochMs)
                .sorted(Comparator.comparingLong(ExecutionSegment::nextDueEpochMs))
                .limit(limit)
                .map(segment -> new DueSegment(
                    segment.tenantId(),
                    segment.runId(),
                    segment.segmentId(),
                    segment.startStepIndex(),
                    segment.stopBeforeStepIndex(),
                    segment.inputPayload()))
                .toList();
        }
    }

    private List<DueBoundaryInteraction> findTimedOutInteractionsBlocking(long nowEpochMs, int limit) {
        ControlPlaneChecks.requireNonNegative(nowEpochMs, "nowEpochMs");
        if (limit <= 0) {
            return List.of();
        }
        synchronized (lock) {
            return projections().stream()
                .flatMap(projection -> projection.interactions().values().stream())
                .filter(interaction -> !interaction.status().terminal())
                .filter(interaction -> interaction.deadlineEpochMs() <= nowEpochMs)
                .sorted(Comparator.comparingLong(BoundaryInteraction::deadlineEpochMs))
                .limit(limit)
                .map(interaction -> new DueBoundaryInteraction(
                    interaction.tenantId(),
                    interaction.runId(),
                    interaction.unitId(),
                    interaction.interactionId(),
                    interaction.kind(),
                    interaction.deadlineEpochMs()))
                .toList();
        }
    }

    private List<ControlPlaneProjection> projections() {
        return eventsByRun.entrySet().stream()
            .map(entry -> ControlPlaneReducer.reduce(entry.getKey().tenantId(), entry.getKey().runId(), entry.getValue()))
            .toList();
    }

    private record Key(String tenantId, String runId) {
        private Key {
            tenantId = ControlPlaneChecks.requireText(tenantId, "tenantId");
            runId = ControlPlaneChecks.requireText(runId, "runId");
        }
    }
}
