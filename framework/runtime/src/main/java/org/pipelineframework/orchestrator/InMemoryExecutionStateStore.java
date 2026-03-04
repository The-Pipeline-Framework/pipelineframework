package org.pipelineframework.orchestrator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final Map<String, ExecutionRecord> executionsByScopedId = new HashMap<>();
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
                    ExecutionRecord existing = executionsByScopedId.get(scopedExecutionId(command.tenantId(), existingExecutionId));
                    if (existing != null) {
                        return new CreateExecutionResult(existing, true);
                    }
                }

                String executionId = UUID.randomUUID().toString();
                ExecutionRecord created = new ExecutionRecord(
                    command.tenantId(),
                    executionId,
                    command.executionKey(),
                    ExecutionStatus.QUEUED,
                    0L,
                    0,
                    0,
                    null,
                    0L,
                    command.nowEpochMs(),
                    null,
                    command.inputPayload(),
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
    public Uni<Optional<ExecutionRecord>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(executionsByScopedId.get(scopedExecutionId(tenantId, executionId)));
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord current = executionsByScopedId.get(scopedId);
                if (current == null || current.status().terminal()) {
                    return Optional.empty();
                }
                boolean leaseExpired = current.leaseOwner() == null || current.leaseExpiresEpochMs() <= nowEpochMs;
                boolean due = current.nextDueEpochMs() <= nowEpochMs;
                if (!leaseExpired || !due) {
                    return Optional.empty();
                }
                ExecutionRecord claimed = new ExecutionRecord(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    ExecutionStatus.RUNNING,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    leaseOwner,
                    nowEpochMs + leaseMs,
                    current.nextDueEpochMs(),
                    current.lastTransitionKey(),
                    current.inputPayload(),
                    current.resultPayload(),
                    current.errorCode(),
                    current.errorMessage(),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                executionsByScopedId.put(scopedId, claimed);
                return Optional.of(claimed);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord current = executionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord updated = new ExecutionRecord(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    ExecutionStatus.SUCCEEDED,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    null,
                    0L,
                    nowEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    resultPayload,
                    null,
                    null,
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord>> scheduleRetry(
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
                ExecutionRecord current = executionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord updated = new ExecutionRecord(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    ExecutionStatus.WAIT_RETRY,
                    current.version() + 1,
                    current.currentStepIndex(),
                    nextAttempt,
                    null,
                    0L,
                    nextDueEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    null,
                    errorCode,
                    truncate(errorMessage),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<Optional<ExecutionRecord>> markTerminalFailure(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs) {
        if (finalStatus != ExecutionStatus.FAILED && finalStatus != ExecutionStatus.DLQ) {
            return Uni.createFrom().item(Optional.empty());
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord current = executionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord updated = new ExecutionRecord(
                    current.tenantId(),
                    current.executionId(),
                    current.executionKey(),
                    finalStatus,
                    current.version() + 1,
                    current.currentStepIndex(),
                    current.attempt(),
                    null,
                    0L,
                    nowEpochMs,
                    transitionKey,
                    current.inputPayload(),
                    null,
                    errorCode,
                    truncate(errorMessage),
                    current.createdAtEpochMs(),
                    nowEpochMs,
                    current.ttlEpochS());
                executionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    @Override
    public Uni<List<ExecutionRecord>> findDueExecutions(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
                List<ExecutionRecord> due = new ArrayList<>();
                for (ExecutionRecord record : executionsByScopedId.values()) {
                    if (record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS) {
                        continue;
                    }
                    if (record.status().terminal()) {
                        continue;
                    }
                    boolean dueNow = record.nextDueEpochMs() <= nowEpochMs;
                    boolean leaseFree = record.leaseOwner() == null || record.leaseExpiresEpochMs() <= nowEpochMs;
                    if (dueNow && leaseFree) {
                        due.add(record);
                    }
                }
                due.sort(Comparator.comparingLong(ExecutionRecord::nextDueEpochMs));
                if (due.size() > limit) {
                    return List.copyOf(due.subList(0, limit));
                }
                return List.copyOf(due);
            }
        });
    }

    private static String scopedExecutionId(String tenantId, String executionId) {
        return tenantId + "|" + executionId;
    }

    private static String scopedExecutionKey(String tenantId, String executionKey) {
        return tenantId + "|" + executionKey;
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
