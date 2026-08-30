package org.pipelineframework.orchestrator;

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

import io.smallrye.mutiny.Uni;

/**
 * In-memory execution state store intended for development and tests.
 */
@ApplicationScoped
public class InMemoryExecutionStateStore implements ExecutionStateStore {

    private final Object lock = new Object();
    private final Map<String, ExecutionRecord<Object, Object>> executionsByScopedId = new HashMap<>();
    private final Map<String, String> executionIdByScopedKey = new HashMap<>();

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedKey = scopedExecutionKey(command.tenantId(), command.executionKey());
                String existingExecutionId = executionIdByScopedKey.get(scopedKey);
                if (existingExecutionId != null) {
                    ExecutionRecord<Object, Object> existing =
                        executionsByScopedId.get(scopedExecutionId(command.tenantId(), existingExecutionId));
                    if (existing != null) {
                        if (!isExpired(existing, command.nowEpochMs())) {
                            return new CreateExecutionResult(existing, true);
                        }
                        executionsByScopedId.remove(scopedExecutionId(command.tenantId(), existing.executionId()));
                    }
                    executionIdByScopedKey.remove(scopedKey);
                }

                String executionId = UUID.randomUUID().toString();
                ExecutionRecord<Object, Object> created = new ExecutionRecord<>(
                    command.tenantId(),
                    executionId,
                    command.executionKey(),
                    command.pipelineId(),
                    command.contractVersion(),
                    command.releaseVersion(),
                    command.resultShape(),
                    ExecutionStatus.QUEUED,
                    0L,
                    command.initialStepIndex(),
                    0,
                    null,
                    0L,
                    command.nowEpochMs(),
                    null,
                    command.inputPayload(),
                    null,
                    null,
                    null,
                    null,
                    command.nowEpochMs(),
                    command.nowEpochMs(),
                    command.ttlEpochS());

                executionIdByScopedKey.put(scopedKey, executionId);
                executionsByScopedId.put(scopedExecutionId(command.tenantId(), executionId), created);
                return new CreateExecutionResult(created, false);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long nowEpochMs = System.currentTimeMillis();
                String scopedId = scopedExecutionId(tenantId, executionId);
                return Optional.ofNullable(getActiveRecord(scopedId, nowEpochMs));
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecutionByKey(String tenantId, String executionKey) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long nowEpochMs = System.currentTimeMillis();
                String executionId = executionIdByScopedKey.get(scopedExecutionKey(tenantId, executionKey));
                if (executionId == null) {
                    return Optional.empty();
                }
                return Optional.ofNullable(getActiveRecord(scopedExecutionId(tenantId, executionId), nowEpochMs));
            }
        });
    }

    @Override
    public Uni<List<Optional<ExecutionRecord<Object, Object>>>> getExecutionsByKey(
        String tenantId,
        List<String> executionKeys
    ) {
        List<String> requestedKeys = List.copyOf(executionKeys);
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long nowEpochMs = System.currentTimeMillis();
                List<Optional<ExecutionRecord<Object, Object>>> records = new ArrayList<>(requestedKeys.size());
                for (String executionKey : requestedKeys) {
                    String executionId = executionIdByScopedKey.get(scopedExecutionKey(tenantId, executionKey));
                    records.add(executionId == null
                        ? Optional.empty()
                        : Optional.ofNullable(getActiveRecord(scopedExecutionId(tenantId, executionId), nowEpochMs)));
                }
                return List.copyOf(records);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null
                    || current.status().terminal()
                    || current.status() == ExecutionStatus.WAITING_EXTERNAL
                    || current.status() == ExecutionStatus.REMOTE_OUTCOME_UNKNOWN) {
                    return Optional.empty();
                }
                boolean leaseExpired = current.leaseOwner() == null || current.leaseExpiresEpochMs() <= nowEpochMs;
                boolean due = current.nextDueEpochMs() <= nowEpochMs;
                if (!leaseExpired || !due) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> claimed = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.RUNNING,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    leaseOwner,
                    nowEpochMs + leaseMs,
                    current.nextDueEpochMs(),
                    current.lastTransitionKey(),
                    current.inputPayload(),
                    current.awaitUnitId(),
                    current.resultPayload(),
                    current.errorCode(),
                    current.errorMessage(),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity(),
                    current.redriveIntent(),
                    current.failedStepIndex(),
                    current.failedCommandId());
                executionsByScopedId.put(scopedId, claimed);
                return Optional.of(claimed);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> renewLease(
        String tenantId,
        String executionId,
        long expectedVersion,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs) {
        if (leaseMs <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("leaseMs must be > 0 for renewLease."));
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null
                    || current.status() != ExecutionStatus.RUNNING
                    || current.version() != expectedVersion
                    || !Objects.equals(current.leaseOwner(), leaseOwner)
                    || current.leaseExpiresEpochMs() <= nowEpochMs) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> renewed = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    current.status(),
                    current.version(),
                    current.currentStepIndex(),
                    current.attempt(),
                    current.leaseOwner(),
                    nowEpochMs + leaseMs,
                    current.nextDueEpochMs(),
                    current.lastTransitionKey(),
                    current.inputPayload(),
                    current.awaitUnitId(),
                    current.resultPayload(),
                    current.errorCode(),
                    current.errorMessage(),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity(),
                    current.redriveIntent(),
                    current.failedStepIndex(),
                    current.failedCommandId());
                executionsByScopedId.put(scopedId, renewed);
                return Optional.of(renewed);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.SUCCEEDED,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    null,
                    0L,
                    nowEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    null,
                    resultPayload,
                    null,
                    null,
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markWaitingExternal(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        String awaitUnitId,
        int awaitStepIndex,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.WAITING_EXTERNAL,
                    current.version() + 1,
                    awaitStepIndex,
                    current.attempt(),
                    null,
                    0L,
                    Long.MAX_VALUE,
                    transitionKey,
                    current.inputPayload(),
                    awaitUnitId,
                    null,
                    null,
                    null,
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitCompleted(
        String tenantId,
        String executionId,
        String awaitUnitId,
        int nextStepIndex,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.status() != ExecutionStatus.WAITING_EXTERNAL) {
                    return Optional.empty();
                }
                if (current.awaitUnitId() != null && !current.awaitUnitId().equals(awaitUnitId)) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.QUEUED,
                    current.version() + 1,
                    nextStepIndex,
                    current.attempt(),
                    null,
                    0L,
                    nowEpochMs,
                    current.lastTransitionKey(),
                    current.inputPayload(),
                    awaitUnitId,
                    null,
                    null,
                    null,
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markAwaitItemContinuationsCompleted(
        String tenantId,
        String executionId,
        String awaitUnitId,
        int nextStepIndex,
        Object inputPayload,
        long nowEpochMs) {
        if (awaitUnitId == null || awaitUnitId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "awaitUnitId must not be blank when releasing await item continuations"));
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.status() != ExecutionStatus.WAITING_EXTERNAL) {
                    return Optional.empty();
                }
                if (!Objects.equals(current.awaitUnitId(), awaitUnitId)) {
                    return Optional.empty();
                }
                ExecutionInputSnapshot normalizedInput = normalizeExecutionInputPayload(inputPayload);
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.QUEUED,
                    current.version() + 1,
                    nextStepIndex,
                    current.attempt(),
                    null,
                    0L,
                    nowEpochMs,
                    current.lastTransitionKey(),
                    normalizedInput,
                    null,
                    null,
                    null,
                    null,
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    private static ExecutionInputSnapshot normalizeExecutionInputPayload(Object inputPayload) {
        if (inputPayload instanceof ExecutionInputSnapshot snapshot) {
            return new ExecutionInputSnapshot(
                snapshot.shape(), copySnapshotPayload(snapshot.payload()), snapshot.pipelineContext());
        }
        return new ExecutionInputSnapshot(ExecutionInputShape.RAW, copySnapshotPayload(inputPayload));
    }

    private static Object copySnapshotPayload(Object payload) {
        if (payload instanceof List<?> list) {
            return List.copyOf(list);
        }
        return payload;
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> scheduleRetry(
        String tenantId,
        String executionId,
        long expectedVersion,
        int nextAttempt,
        long nextDueEpochMs,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.WAIT_RETRY,
                    current.version() + 1,
                    current.currentStepIndex(),
                    nextAttempt,
                    null,
                    0L,
                    nextDueEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    current.awaitUnitId(),
                    null,
                    errorCode,
                    truncate(errorMessage),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markRemoteOutcomeUnknown(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.REMOTE_OUTCOME_UNKNOWN,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    null,
                    0L,
                    Long.MAX_VALUE,
                    transitionKey,
                    current.inputPayload(),
                    current.awaitUnitId(),
                    null,
                    errorCode,
                    truncate(errorMessage),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> deferCircuit(
        String tenantId,
        String executionId,
        long expectedVersion,
        long nextDueEpochMs,
        String transitionKey,
        String circuitIdentity,
        String reason,
        String errorMessage,
        long firstCircuitDeferredAtEpochMs,
        int circuitDeferralCount,
        long nowEpochMs
    ) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(), current.executionId(), current.executionKey(), current.pipelineId(),
                    current.contractVersion(), current.releaseVersion(), current.resultShape(), ExecutionStatus.WAIT_RETRY,
                    current.version() + 1, current.currentStepIndex(), current.attempt(), null, 0L, nextDueEpochMs,
                    transitionKey, current.inputPayload(), current.awaitUnitId(), null, reason, truncate(errorMessage),
                    current.createdAtEpochMs(), nowEpochMs, current.ttlEpochS(), firstCircuitDeferredAtEpochMs,
                    circuitDeferralCount, circuitIdentity == null ? "" : circuitIdentity);
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        return markTerminalFailure(
            tenantId, executionId, expectedVersion, finalStatus, transitionKey,
            errorCode, errorMessage, -1, Optional.empty(), nowEpochMs);
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        int failedStepIndex,
        long nowEpochMs) {
        return markTerminalFailure(
            tenantId, executionId, expectedVersion, finalStatus, transitionKey,
            errorCode, errorMessage, failedStepIndex, Optional.empty(), nowEpochMs);
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        int failedStepIndex,
        Optional<String> failedCommandId,
        long nowEpochMs) {
        if (finalStatus != ExecutionStatus.FAILED && finalStatus != ExecutionStatus.DLQ) {
            return Uni.createFrom().item(Optional.empty());
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    finalStatus,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    null,
                    0L,
                    nowEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    current.awaitUnitId(),
                    null,
                    errorCode,
                    truncate(errorMessage),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity(),
                    ExecutionRedriveIntent.REPLAY,
                    failedStepIndex,
                    failedCommandId);
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> redriveTerminalExecution(
        String tenantId,
        String executionId,
        long expectedVersion,
        boolean allowFailed,
        String transitionKey,
        long nowEpochMs) {
        return redriveTerminalExecution(
            tenantId,
            executionId,
            expectedVersion,
            allowFailed,
            ExecutionRedriveIntent.REPLAY,
            transitionKey,
            nowEpochMs);
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> redriveTerminalExecution(
        String tenantId,
        String executionId,
        long expectedVersion,
        boolean allowFailed,
        ExecutionRedriveIntent intent,
        String transitionKey,
        long nowEpochMs) {
        ExecutionRedriveIntent resolvedIntent = Objects.requireNonNull(intent, "intent must not be null");
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = getActiveRecord(scopedId, nowEpochMs);
                if (current == null
                    || current.version() != expectedVersion
                    || !redrivable(current.status(), allowFailed)) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    current.pipelineId(),
                    current.contractVersion(),
                    current.releaseVersion(),
                    current.resultShape(),
                    ExecutionStatus.QUEUED,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt() + 1,
                    null,
                    0L,
                    nowEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    current.awaitUnitId(),
                    null,
                    null,
                    null,
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS(),
                    current.firstCircuitDeferredAtEpochMs(),
                    current.circuitDeferralCount(),
                    current.circuitIdentity(),
                    resolvedIntent,
                    current.failedStepIndex(),
                    current.failedCommandId());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                List<ExecutionRecord<Object, Object>> due = new ArrayList<>();
                var iterator = executionsByScopedId.entrySet().iterator();
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    ExecutionRecord<Object, Object> record = entry.getValue();
                    if (isExpired(record, nowEpochMs)) {
                        iterator.remove();
                        executionIdByScopedKey.remove(scopedExecutionKey(record.tenantId(), record.executionKey()));
                        continue;
                    }
                    if (record.status().terminal()
                        || record.status() == ExecutionStatus.WAITING_EXTERNAL
                        || record.status() == ExecutionStatus.REMOTE_OUTCOME_UNKNOWN) {
                        continue;
                    }
                    boolean dueNow = record.nextDueEpochMs() <= nowEpochMs;
                    boolean leaseFree = record.leaseOwner() == null || record.leaseExpiresEpochMs() <= nowEpochMs;
                    if (dueNow && leaseFree) {
                        due.add(record);
                    }
                }
                due.sort(Comparator.comparingLong(ExecutionRecord::nextDueEpochMs));
                if (limit <= 0) {
                    return List.of();
                }
                if (due.size() > limit) {
                    return List.copyOf(due.subList(0, limit));
                }
                return List.copyOf(due);
            }
        });
    }

    private static boolean isExpired(ExecutionRecord<Object, Object> record, long nowEpochMs) {
        long ttl = record.ttlEpochS();
        if (ttl <= 0) {
            return false;
        }
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        return ttl <= nowEpochS;
    }

    private static boolean redrivable(ExecutionStatus status, boolean allowFailed) {
        return status == ExecutionStatus.DLQ
            || (allowFailed && (status == ExecutionStatus.FAILED
                || status == ExecutionStatus.REMOTE_OUTCOME_UNKNOWN));
    }

    private ExecutionRecord<Object, Object> getActiveRecord(String scopedId, long nowEpochMs) {
        ExecutionRecord<Object, Object> current = executionsByScopedId.get(scopedId);
        if (current == null) {
            return null;
        }
        if (!isExpired(current, nowEpochMs)) {
            return current;
        }
        executionsByScopedId.remove(scopedId);
        executionIdByScopedKey.remove(scopedExecutionKey(current.tenantId(), current.executionKey()));
        return null;
    }

    private static String scopedExecutionId(String tenantId, String executionId) {
        return compositeScopedKey("tenantId", tenantId, "executionId", executionId);
    }

    private static String scopedExecutionKey(String tenantId, String executionKey) {
        return compositeScopedKey("tenantId", tenantId, "executionKey", executionKey);
    }

    private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
        String safeLeft = Objects.requireNonNull(left, leftName + " must not be null");
        String safeRight = Objects.requireNonNull(right, rightName + " must not be null");
        return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
    }

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() <= 512) {
            return value;
        }
        return value.substring(0, 512);
    }
}
