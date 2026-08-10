package org.pipelineframework.awaitable.store;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionNotFoundException;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitContinuationStatus;
import org.pipelineframework.awaitable.AwaitInteractionTerminalException;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.InMemoryControlPlaneTransactionLock;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.stream.StreamRegionPageCommit;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;

/**
 * In-memory await store intended for local development and tests.
 */
@ApplicationScoped
public class InMemoryAwaitInteractionStore implements AwaitInteractionStore {

    private static final Comparator<AwaitInteractionRecord> PENDING_ORDER =
        Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs)
            .thenComparingLong(AwaitInteractionRecord::createdAtEpochMs)
            .thenComparing(AwaitInteractionRecord::interactionId);

    private Object lock = new Object();
    private final Map<String, AwaitInteractionRecord> interactionsByScopedId = new HashMap<>();
    private final Map<String, String> interactionIdByScopedIdempotencyKey = new HashMap<>();
    private final Map<String, String> interactionIdByScopedCorrelation = new HashMap<>();

    @Inject
    Instance<ExecutionStateStore> executionStateStores;

    private InMemoryExecutionStateStore explicitExecutionStateStore;

    public InMemoryAwaitInteractionStore() {
    }

    public InMemoryAwaitInteractionStore(
        InMemoryControlPlaneTransactionLock transactionLock,
        InMemoryExecutionStateStore executionStateStore
    ) {
        bindTransactionLock(transactionLock);
        explicitExecutionStateStore = Objects.requireNonNull(executionStateStore, "executionStateStore must not be null");
    }

    @Inject
    void bindTransactionLock(InMemoryControlPlaneTransactionLock transactionLock) {
        lock = Objects.requireNonNull(transactionLock, "transactionLock must not be null").monitor();
    }

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                String scopedKey = scopedIdempotencyKey(command.tenantId(), command.stepId(), command.idempotencyKey());
                String existingId = interactionIdByScopedIdempotencyKey.get(scopedKey);
                if (existingId != null) {
                    AwaitInteractionRecord existing = interactionsByScopedId.get(scopedInteractionId(command.tenantId(), existingId));
                    if (existing != null && !existing.status().terminal()) {
                        return new AwaitCreateResult(existing, true);
                    }
                }
                String interactionId = UUID.randomUUID().toString();
                AwaitInteractionRecord created = new AwaitInteractionRecord(
                    command.tenantId(),
                    command.executionId(),
                    command.stepId(),
                    command.stepIndex(),
                    command.outputType(),
                    interactionId,
                    command.correlationId(),
                    command.causationId(),
                    command.idempotencyKey(),
                    0L,
                    AwaitInteractionStatus.WAITING,
                    command.requestPayload(),
                    null,
                    command.unitId(),
                    command.itemIndex(),
                    null,
                    command.assignee(),
                    command.group(),
                    command.transportType(),
                    Map.of(),
                    command.deadlineEpochMs(),
                    command.nowEpochMs(),
                    command.nowEpochMs(),
                    command.ttlEpochS(),
                    command.transportOutputType());
                interactionsByScopedId.put(scopedInteractionId(created.tenantId(), created.interactionId()), created);
                interactionIdByScopedIdempotencyKey.put(scopedKey, interactionId);
                interactionIdByScopedCorrelation.put(scopedCorrelation(command.tenantId(), command.correlationId()), interactionId);
                return new AwaitCreateResult(created, false);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                return Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId)));
            }
        });
    }

    @Override
    public Uni<AwaitInteractionRecord> importRecord(AwaitInteractionRecord record) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String scopedId = scopedInteractionId(record.tenantId(), record.interactionId());
                AwaitInteractionRecord existing = interactionsByScopedId.get(scopedId);
                if (existing != null) {
                    return existing;
                }
                interactionsByScopedId.put(scopedId, record);
                interactionIdByScopedIdempotencyKey.put(
                    scopedIdempotencyKey(record.tenantId(), record.stepId(), record.idempotencyKey()),
                    record.interactionId());
                interactionIdByScopedCorrelation.put(
                    scopedCorrelation(record.tenantId(), record.correlationId()),
                    record.interactionId());
                return record;
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String interactionId = interactionIdByScopedCorrelation.get(scopedCorrelation(tenantId, correlationId));
                return interactionId == null
                    ? Optional.empty()
                    : Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId)));
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findByUnit(
        String tenantId,
        String unitId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                return interactionsByScopedId.values().stream()
                    .filter(record -> Objects.equals(record.tenantId(), tenantId))
                    .filter(record -> Objects.equals(record.unitId(), unitId))
                    .sorted(Comparator
                        .comparingInt((AwaitInteractionRecord record) -> record.itemIndex() == null
                            ? Integer.MAX_VALUE
                            : record.itemIndex())
                        .thenComparing(record -> nullToEmpty(record.causationId()))
                        .thenComparing(AwaitInteractionRecord::interactionId))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.WAITING,
            current -> updateStatus(current, AwaitInteractionStatus.DISPATCHING, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatched(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        Map<String, Object> safeMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.DISPATCHING,
            current -> new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            AwaitInteractionStatus.DISPATCHED,
            current.requestPayload(),
            current.responsePayload(),
            current.unitId(),
            current.itemIndex(),
            current.actor(),
            current.assignee(),
            current.group(),
            current.transportType(),
            safeMetadata,
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS(),
            current.transportOutputType(), current.continuationStatus(), current.continuationAttempt(),
            current.continuationNextDueEpochMs(), current.continuationLeaseOwner(),
            current.continuationLeaseExpiresEpochMs(), current.continuationOutputPayload(), current.streamRegionId()));
    }

    @Override
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                AwaitInteractionRecord current = resolveForCompletion(command)
                    .orElseThrow(() -> new AwaitInteractionNotFoundException("No await interaction matches completion"));
                if (current.status() == AwaitInteractionStatus.COMPLETED) {
                    return new AwaitCompletionResult(current, true);
                }
                if (current.status().terminal()) {
                    throw new AwaitInteractionTerminalException("Await interaction is terminal: " + current.status());
                }
                if (current.deadlineEpochMs() <= command.nowEpochMs()) {
                    AwaitInteractionRecord timedOut = updateStatus(current, AwaitInteractionStatus.TIMED_OUT, command.nowEpochMs(), null, null);
                    interactionsByScopedId.put(scopedInteractionId(timedOut.tenantId(), timedOut.interactionId()), timedOut);
                    throw new AwaitInteractionTerminalException("Await interaction timed out before completion");
                }
                AwaitInteractionRecord completed = new AwaitInteractionRecord(
                    current.tenantId(),
                    current.executionId(),
                    current.stepId(),
                    current.stepIndex(),
                    current.outputType(),
                    current.interactionId(),
                    current.correlationId(),
                    current.causationId(),
                    current.idempotencyKey(),
                    current.version() + 1,
                    AwaitInteractionStatus.COMPLETED,
                    current.requestPayload(),
                    command.responsePayload(),
                    current.unitId(),
                    current.itemIndex(),
                    command.actor(),
                    current.assignee(),
                    current.group(),
                    current.transportType(),
                    current.transportMetadata(),
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            command.nowEpochMs(),
            current.ttlEpochS(),
            current.transportOutputType(), current.continuationStatus(), current.continuationAttempt(),
            current.continuationNextDueEpochMs(), current.continuationLeaseOwner(),
            current.continuationLeaseExpiresEpochMs(), current.continuationOutputPayload(), current.streamRegionId());
                interactionsByScopedId.put(scopedInteractionId(completed.tenantId(), completed.interactionId()), completed);
                return new AwaitCompletionResult(completed, false);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> activateContinuationIfEligible(
        String tenantId, String interactionId, long expectedVersion, long nowEpochMs) {
        return continuationTransition(tenantId, interactionId, expectedVersion, nowEpochMs, current -> {
            if (!current.itemInteraction() || current.status() != AwaitInteractionStatus.COMPLETED
                || current.continuationStatus() != AwaitContinuationStatus.HELD) {
                return Optional.empty();
            }
            return Optional.of(withContinuation(current, AwaitContinuationStatus.READY,
                current.continuationAttempt(), nowEpochMs, "", 0L, current.continuationOutputPayload(), nowEpochMs));
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> claimDueContinuation(
        String tenantId, String interactionId, String leaseOwner, long nowEpochMs, long leaseMs) {
        if (leaseOwner == null || leaseOwner.isBlank() || leaseMs <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("continuation lease owner and duration are required"));
        }
        return continuationTransition(tenantId, interactionId, -1L, nowEpochMs, current -> {
            if (!current.continuationDue(nowEpochMs)) {
                return Optional.empty();
            }
            return Optional.of(withContinuation(current, AwaitContinuationStatus.CLAIMED,
                current.continuationAttempt() + 1, current.continuationNextDueEpochMs(), leaseOwner,
                nowEpochMs + leaseMs, current.continuationOutputPayload(), nowEpochMs));
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findDueContinuations(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                return interactionsByScopedId.values().stream()
                    .filter(record -> record.continuationDue(nowEpochMs))
                    .sorted(Comparator.comparingLong(AwaitInteractionRecord::continuationNextDueEpochMs)
                        .thenComparing(AwaitInteractionRecord::interactionId))
                    .limit(Math.max(0, limit))
                    .toList();
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findDueStreamInteractionDispatches(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                return interactionsByScopedId.values().stream()
                    .filter(record -> record.status() == AwaitInteractionStatus.WAITING)
                    .filter(record -> !record.streamRegionId().isBlank())
                    .filter(record -> record.createdAtEpochMs() <= nowEpochMs)
                    .sorted(Comparator.comparingLong(AwaitInteractionRecord::createdAtEpochMs)
                        .thenComparing(AwaitInteractionRecord::tenantId)
                        .thenComparing(AwaitInteractionRecord::interactionId))
                    .limit(Math.max(0, limit))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> rescheduleContinuation(
        String tenantId, String interactionId, long expectedVersion, long nextDueEpochMs, long nowEpochMs) {
        return continuationTransition(tenantId, interactionId, expectedVersion, nowEpochMs, current -> {
            if (current.continuationStatus() != AwaitContinuationStatus.CLAIMED) {
                return Optional.empty();
            }
            return Optional.of(withContinuation(current, AwaitContinuationStatus.RETRY_DUE,
                current.continuationAttempt(), Math.max(nowEpochMs, nextDueEpochMs), "", 0L,
                current.continuationOutputPayload(), nowEpochMs));
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> completeContinuation(
        String tenantId, String interactionId, long expectedVersion, Object outputPayload, long nowEpochMs) {
        return continuationTransition(tenantId, interactionId, expectedVersion, nowEpochMs, current -> {
            if (current.continuationStatus() != AwaitContinuationStatus.CLAIMED) {
                return Optional.empty();
            }
            return Optional.of(withContinuation(current, AwaitContinuationStatus.APPLIED,
                current.continuationAttempt(), 0L, "", 0L, outputPayload, nowEpochMs));
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> completeContinuationAndReleaseStreamCredit(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String leaseOwner,
        Object outputPayload,
        long nowEpochMs
    ) {
        if (leaseOwner == null || leaseOwner.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("continuation lease owner must not be blank"));
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                AwaitInteractionRecord current = interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId));
                if (current == null
                    || current.version() != expectedVersion
                    || current.status() != AwaitInteractionStatus.COMPLETED
                    || current.continuationStatus() != AwaitContinuationStatus.CLAIMED
                    || !leaseOwner.equals(current.continuationLeaseOwner())
                    || current.streamRegionId().isBlank()) {
                    return Optional.empty();
                }
                AwaitInteractionRecord applied = withContinuation(
                    current, AwaitContinuationStatus.APPLIED, current.continuationAttempt(),
                    0L, "", 0L, outputPayload, nowEpochMs);
                Optional<StreamRegionRecord> released = inMemoryExecutionStateStore()
                    .releaseStreamRegionCreditInTransaction(
                        current.tenantId(), current.executionId(), current.streamRegionId(), nowEpochMs);
                if (released.isEmpty()) {
                    return Optional.empty();
                }
                if (released.get().status().terminal() && released.get().terminalScalarSuffix()
                    && !inMemoryExecutionStateStore().completeStreamRegionParentInTransaction(
                        current.tenantId(), current.executionId(), current.unitId(),
                        "stream-region-complete:" + current.streamRegionId(), nowEpochMs)) {
                    return Optional.empty();
                }
                interactionsByScopedId.put(scopedInteractionId(tenantId, interactionId), applied);
                return Optional.of(applied);
            }
        });
    }

    @Override
    public Uni<Optional<StreamRegionRecord>> materializeStreamRegionPage(StreamRegionPageCommit commit) {
        Objects.requireNonNull(commit, "commit must not be null");
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(commit.nowEpochMs());
                StreamRegionRecord expected = commit.claimedRegion().recordPage(
                    commit.nextCheckpoint(), commit.interactions().size(), commit.endOfSource(), commit.nowEpochMs());
                Optional<StreamRegionRecord> existing = inMemoryExecutionStateStore()
                    .getStreamRegionInTransaction(commit.claimedRegion().tenantId(), commit.claimedRegion().executionId(),
                        commit.claimedRegion().regionId());
                if (existing.filter(region -> pageCommitRecovered(region, expected, commit.interactions())).isPresent()) {
                    return Optional.of(expected);
                }
                if (commit.interactions().stream().anyMatch(interaction -> interactionAlreadyExists(interaction)
                    || interactionLookupExists(interaction))) {
                    return Optional.empty();
                }
                Optional<StreamRegionRecord> updated = inMemoryExecutionStateStore()
                    .recordStreamRegionPageInTransaction(commit);
                if (updated.isEmpty()) {
                    return Optional.empty();
                }
                for (AwaitInteractionRecord interaction : commit.interactions()) {
                    interactionsByScopedId.put(scopedInteractionId(interaction.tenantId(), interaction.interactionId()), interaction);
                    interactionIdByScopedIdempotencyKey.put(
                        scopedIdempotencyKey(interaction.tenantId(), interaction.stepId(), interaction.idempotencyKey()),
                        interaction.interactionId());
                    interactionIdByScopedCorrelation.put(
                        scopedCorrelation(interaction.tenantId(), interaction.correlationId()), interaction.interactionId());
                }
                return updated;
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> fail(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.FAILED, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> cancel(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.CANCELLED, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.TIMED_OUT, nowEpochMs, null, null));
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                List<AwaitInteractionRecord> records = interactionsByScopedId.values().stream()
                    .filter(record -> !record.status().terminal())
                    .filter(record -> record.deadlineEpochMs() <= nowEpochMs)
                    .sorted(PENDING_ORDER)
                    .limit(Math.max(0, limit))
                    .toList();
                return List.copyOf(records);
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String normalizedAssignee = normalizeFilter(assignee);
                String normalizedGroup = normalizeFilter(group);
                String normalizedStepId = normalizeFilter(stepId);
                List<AwaitInteractionRecord> records = new ArrayList<>();
                for (AwaitInteractionRecord record : interactionsByScopedId.values()) {
                    if (record.status().terminal() || !Objects.equals(record.tenantId(), tenantId)) {
                        continue;
                    }
                    if (normalizedAssignee != null && !Objects.equals(normalizedAssignee, record.assignee())) {
                        continue;
                    }
                    if (normalizedGroup != null && !Objects.equals(normalizedGroup, record.group())) {
                        continue;
                    }
                    if (normalizedStepId != null && !Objects.equals(normalizedStepId, record.stepId())) {
                        continue;
                    }
                    records.add(record);
                }
                records.sort(PENDING_ORDER);
                return List.copyOf(records.subList(0, Math.min(records.size(), Math.max(0, limit))));
            }
        });
    }

    private Uni<Optional<AwaitInteractionRecord>> transition(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs,
        AwaitInteractionStatus requiredStatus,
        java.util.function.Function<AwaitInteractionRecord, AwaitInteractionRecord> transition) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedInteractionId(tenantId, interactionId);
                AwaitInteractionRecord current = interactionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion || current.status().terminal()) {
                    return Optional.empty();
                }
                if (requiredStatus != null && current.status() != requiredStatus) {
                    return Optional.empty();
                }
                AwaitInteractionRecord updated = transition.apply(current);
                interactionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    private Optional<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        if (command.interactionId() != null && !command.interactionId().isBlank()) {
            return Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(command.tenantId(), command.interactionId())));
        }
        String interactionId = interactionIdByScopedCorrelation.get(scopedCorrelation(command.tenantId(), command.correlationId()));
        return interactionId == null
            ? Optional.empty()
            : Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(command.tenantId(), interactionId)));
    }

    private Uni<Optional<AwaitInteractionRecord>> continuationTransition(
        String tenantId, String interactionId, long expectedVersion, long nowEpochMs,
        java.util.function.Function<AwaitInteractionRecord, Optional<AwaitInteractionRecord>> transition) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedInteractionId(tenantId, interactionId);
                AwaitInteractionRecord current = interactionsByScopedId.get(scopedId);
                if (current == null || (expectedVersion >= 0 && current.version() != expectedVersion)) {
                    return Optional.empty();
                }
                Optional<AwaitInteractionRecord> updated = transition.apply(current);
                updated.ifPresent(record -> interactionsByScopedId.put(scopedId, record));
                return updated;
            }
        });
    }

    private AwaitInteractionRecord updateStatus(
        AwaitInteractionRecord current,
        AwaitInteractionStatus status,
        long nowEpochMs,
        Object responsePayload,
        String actor) {
        return new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            status,
            current.requestPayload(),
            responsePayload == null ? current.responsePayload() : responsePayload,
            current.unitId(),
            current.itemIndex(),
            actor == null ? current.actor() : actor,
            current.assignee(),
            current.group(),
            current.transportType(),
            current.transportMetadata(),
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS(),
            current.transportOutputType(), current.continuationStatus(), current.continuationAttempt(),
            current.continuationNextDueEpochMs(), current.continuationLeaseOwner(),
            current.continuationLeaseExpiresEpochMs(), current.continuationOutputPayload(), current.streamRegionId());
    }

    private static AwaitInteractionRecord withContinuation(
        AwaitInteractionRecord current, AwaitContinuationStatus continuationStatus, int continuationAttempt,
        long continuationNextDueEpochMs, String continuationLeaseOwner, long continuationLeaseExpiresEpochMs,
        Object continuationOutputPayload, long nowEpochMs) {
        return new AwaitInteractionRecord(
            current.tenantId(), current.executionId(), current.stepId(), current.stepIndex(), current.outputType(),
            current.interactionId(), current.correlationId(), current.causationId(), current.idempotencyKey(),
            current.version() + 1, current.status(), current.requestPayload(), current.responsePayload(), current.unitId(),
            current.itemIndex(), current.actor(), current.assignee(), current.group(), current.transportType(),
            current.transportMetadata(), current.deadlineEpochMs(), current.createdAtEpochMs(), nowEpochMs,
            current.ttlEpochS(), current.transportOutputType(), continuationStatus, continuationAttempt,
            continuationNextDueEpochMs, continuationLeaseOwner, continuationLeaseExpiresEpochMs,
            continuationOutputPayload, current.streamRegionId());
    }

    private void purgeExpired(long nowEpochMs) {
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        var iterator = interactionsByScopedId.entrySet().iterator();
        while (iterator.hasNext()) {
            AwaitInteractionRecord record = iterator.next().getValue();
            if (record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS) {
                iterator.remove();
                interactionIdByScopedIdempotencyKey.remove(scopedIdempotencyKey(
                    record.tenantId(), record.stepId(), record.idempotencyKey()));
                interactionIdByScopedCorrelation.remove(scopedCorrelation(record.tenantId(), record.correlationId()));
            }
        }
    }

    private InMemoryExecutionStateStore inMemoryExecutionStateStore() {
        if (explicitExecutionStateStore != null) {
            return explicitExecutionStateStore;
        }
        if (executionStateStores == null) {
            throw new IllegalStateException(
                "In-memory stream-linked await work requires the selectable in-memory execution state store");
        }
        return executionStateStores.stream()
            .filter(InMemoryExecutionStateStore.class::isInstance)
            .map(InMemoryExecutionStateStore.class::cast)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "In-memory stream-linked await work requires InMemoryExecutionStateStore"));
    }

    private boolean interactionAlreadyExists(AwaitInteractionRecord interaction) {
        return interactionsByScopedId.containsKey(scopedInteractionId(interaction.tenantId(), interaction.interactionId()));
    }

    private boolean interactionLookupExists(AwaitInteractionRecord interaction) {
        return interactionIdByScopedIdempotencyKey.containsKey(
                scopedIdempotencyKey(interaction.tenantId(), interaction.stepId(), interaction.idempotencyKey()))
            || interactionIdByScopedCorrelation.containsKey(
                scopedCorrelation(interaction.tenantId(), interaction.correlationId()));
    }

    private boolean pageCommitRecovered(
        StreamRegionRecord stored,
        StreamRegionRecord expected,
        List<AwaitInteractionRecord> interactions
    ) {
        return stored.version() == expected.version()
            && stored.nextLogicalOrdinal() == expected.nextLogicalOrdinal()
            && stored.checkpoint().equals(expected.checkpoint())
            && interactions.stream().allMatch(interaction -> interaction.equals(
                interactionsByScopedId.get(scopedInteractionId(interaction.tenantId(), interaction.interactionId()))));
    }

    private static String scopedInteractionId(String tenantId, String interactionId) {
        return compositeScopedKey("tenantId", tenantId, "interactionId", interactionId);
    }

    private static String scopedIdempotencyKey(String tenantId, String stepId, String idempotencyKey) {
        return compositeScopedKey("tenantStep", tenantId + ":" + stepId, "idempotencyKey", idempotencyKey);
    }

    private static String scopedCorrelation(String tenantId, String correlationId) {
        return compositeScopedKey("tenantId", tenantId, "correlationId", correlationId);
    }

    private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
        String safeLeft = Objects.requireNonNull(left, leftName + " must not be null");
        String safeRight = Objects.requireNonNull(right, rightName + " must not be null");
        return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String normalizeFilter(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
