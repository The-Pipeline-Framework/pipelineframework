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

    /**
     * Provider identifier for this in-memory execution state store.
     *
     * @return the provider name "memory"
     */
    @Override
    public String providerName() {
        return "memory";
    }

    /**
     * Priority for selecting this ExecutionStateStore implementation.
     *
     * @return the provider priority; lower values indicate lower selection preference
     */
    @Override
    public int priority() {
        return -100;
    }

    /**
     * Create a new execution or retrieve an existing execution for the same tenant and execution key.
     *
     * @param command the execution creation command containing tenantId, executionKey, timestamps, payloads, and TTL
     * @return a CreateExecutionResult with the execution record and `true` in the existing flag if an execution with the same tenant and executionKey already existed, `false` otherwise
     */
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

    /**
     * Retrieve the execution record for the specified tenant and execution ID.
     *
     * @return an Optional containing the ExecutionRecord for the given tenant and execution ID, or empty if none exists
     */
    @Override
    public Uni<Optional<ExecutionRecord>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(executionsByScopedId.get(scopedExecutionId(tenantId, executionId)));
            }
        });
    }

    /**
     * Attempt to claim a lease for the specified execution if it is due and not currently leased.
     *
     * @param tenantId    the tenant identifier owning the execution
     * @param executionId the execution identifier to claim
     * @param leaseOwner  identifier for the entity requesting the lease
     * @param nowEpochMs  current time in epoch milliseconds used to evaluate due/expiry
     * @param leaseMs     lease duration in milliseconds to apply if claim succeeds
     * @return            an Optional containing the updated ExecutionRecord with RUNNING status if the lease was claimed, or empty otherwise
     */
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

    /**
     * Mark the specified execution as succeeded if its current version matches the provided expected version.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the execution identifier
     * @param expectedVersion the expected current version; the update is performed only if this matches the stored version
     * @param transitionKey a key identifying the transition that caused the success
     * @param resultPayload the result payload to attach to the execution record
     * @param nowEpochMs the current epoch milliseconds used to update timestamps on the record
     * @return an Optional containing the updated ExecutionRecord if the version matched and the record was updated, `Optional.empty()` otherwise
     */
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

    /**
     * Schedule a retry for an existing execution by updating its state to WAIT_RETRY.
     *
     * @param tenantId the tenant identifier of the execution
     * @param executionId the unique execution identifier
     * @param expectedVersion the expected current version of the execution; update occurs only if this matches
     * @param nextAttempt the attempt number to set for the next retry
     * @param nextDueEpochMs the epoch millis when the next attempt should be due
     * @param transitionKey an identifier for the transition causing the retry
     * @param errorCode an error code associated with this retry scheduling
     * @param errorMessage an error message associated with this retry; will be truncated to 512 characters if longer
     * @param nowEpochMs the current epoch millis used to set the updated timestamp
     * @return an {@code Optional} containing the updated {@code ExecutionRecord} when the version matches and the retry is scheduled, {@code Optional.empty()} otherwise
     */
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

    /**
     * Mark the specified execution as a terminal failure and persist the terminal state when the stored version matches the expected version.
     *
     * @param tenantId the tenant that owns the execution
     * @param executionId the id of the execution to update
     * @param expectedVersion the expected current version for optimistic concurrency; update occurs only if it matches
     * @param finalStatus the terminal status to set; only `FAILED` or `DLQ` are accepted
     * @param transitionKey an optional key describing the transition that produced the terminal state
     * @param errorCode an optional machine-readable error code
     * @param errorMessage an optional human-readable error message (will be truncated to 512 characters if longer)
     * @param nowEpochMs the current epoch time in milliseconds used to set update timestamps
     * @return an `Optional` containing the updated `ExecutionRecord` if the execution was found and updated; `Optional.empty()` if the execution was not found, the version did not match, or `finalStatus` is not `FAILED` or `DLQ`
     */
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

    /**
     * Finds executions that are due to run and are not terminal or currently leased, returning up to a maximum number.
     *
     * Filters executions by TTL and terminal status, selects those with nextDueEpochMs less than or equal to the provided
     * time and with no active lease, then orders results by nextDueEpochMs ascending.
     *
     * @param nowEpochMs epoch milliseconds representing "now" used to evaluate due and lease expiry
     * @param limit the maximum number of executions to return
     * @return a list of matching ExecutionRecord objects ordered by `nextDueEpochMs` (earliest first), containing at most `limit` entries
     */
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

    /**
     * Build a composite scoped execution identifier for a tenant.
     *
     * @param tenantId    the tenant identifier
     * @param executionId the execution identifier
     * @return the composite key in the form "tenantId|executionId"
     */
    private static String scopedExecutionId(String tenantId, String executionId) {
        return tenantId + "|" + executionId;
    }

    /**
     * Builds a composite scoped execution key by joining the tenant ID and execution key with a pipe separator.
     *
     * @param tenantId     the tenant identifier
     * @param executionKey the execution key within the tenant
     * @return             the composite scoped key in the form "tenantId|executionKey"
     */
    private static String scopedExecutionKey(String tenantId, String executionKey) {
        return tenantId + "|" + executionKey;
    }

    /**
     * Truncates the input string to at most 512 characters.
     *
     * @param value the string to truncate; may be null
     * @return the original string if its length is less than or equal to 512, the first 512 characters if longer, or `null` if the input is `null`
     */
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
