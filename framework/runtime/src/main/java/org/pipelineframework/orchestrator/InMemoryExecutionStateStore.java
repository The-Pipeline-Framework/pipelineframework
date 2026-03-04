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

    /**
     * Provider name identifying this in-memory execution state store.
     *
     * @return the provider name "memory"
     */
    @Override
    public String providerName() {
        return "memory";
    }

    /**
     * Provides the provider priority used to order ExecutionStateStore implementations.
     *
     * @return an integer priority value where lower numbers indicate lower priority; this implementation returns -100
     */
    @Override
    public int priority() {
        return -100;
    }

    /**
     * Create a new execution record for the given tenant and execution key, or return the existing record if one already exists for that scoped key.
     *
     * @param command command containing tenant id, execution key, input payload, current timestamp, and TTL used to create the execution when absent
     * @return CreateExecutionResult containing the execution record and a boolean flag: `true` if the record already existed, `false` if a new record was created
     */
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
                        return new CreateExecutionResult(existing, true);
                    }
                }

                String executionId = UUID.randomUUID().toString();
                ExecutionRecord<Object, Object> created = new ExecutionRecord<>(
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
     * Retrieve the execution record for the given tenant and execution identifier.
     *
     * @param tenantId    the tenant identifier
     * @param executionId the execution identifier
     * @return an Optional containing the execution record if present, otherwise an empty Optional
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                return Optional.ofNullable(executionsByScopedId.get(scopedExecutionId(tenantId, executionId)));
            }
        });
    }

    /**
     * Attempt to acquire a lease for the specified execution if it is due and not currently leased.
     *
     * @param tenantId    the tenant that owns the execution
     * @param executionId the id of the execution to claim
     * @param leaseOwner  identifier of the entity attempting to acquire the lease
     * @param nowEpochMs  current time in milliseconds since the Unix epoch
     * @param leaseMs     lease duration in milliseconds to grant if claim succeeds
     * @return            an Optional containing the updated ExecutionRecord with status RUNNING if the lease was acquired, `Optional.empty()` otherwise
     */
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
                ExecutionRecord<Object, Object> current = executionsByScopedId.get(scopedId);
                if (current == null || current.status().terminal()) {
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
     * Mark the specified execution as succeeded and persist the updated execution record when the provided version matches the current version.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the execution identifier to update
     * @param expectedVersion the expected current version used for optimistic concurrency control; update occurs only if this matches the stored version
     * @param transitionKey a key describing the transition that led to success
     * @param resultPayload the result payload to store with the succeeded execution
     * @param nowEpochMs the timestamp (milliseconds since epoch) to record as the update time
     * @return an Optional containing the updated ExecutionRecord if the version matched and the update was applied, `Optional.empty()` otherwise
     */
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
                ExecutionRecord<Object, Object> current = executionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
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
     * Schedules a retry for an existing execution by updating its status to WAIT_RETRY and persisting retry metadata.
     *
     * Updates the execution only if a record exists for the given tenantId/executionId and its version matches expectedVersion.
     *
     * @param tenantId the tenant that owns the execution
     * @param executionId the identifier of the execution to update
     * @param expectedVersion the current version expected for optimistic concurrency; update is applied only if it matches
     * @param nextAttempt the attempt number to set on the execution
     * @param nextDueEpochMs the epoch millisecond timestamp when the execution should next be eligible to run
     * @param transitionKey an identifier for the transition that caused the retry
     * @param errorCode an error code to record for the retry
     * @param errorMessage an error message to record for the retry; will be truncated to 512 characters if longer
     * @param nowEpochMs the current epoch millisecond timestamp to record as the update time
     * @return an Optional containing the updated ExecutionRecord when the update was applied, or Optional.empty() if no matching record exists or the version did not match
     */
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
                ExecutionRecord<Object, Object> current = executionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
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
     * Mark the specified execution as a terminal failure (FAILED or DLQ) and persist the update if the current version matches.
     *
     * @param tenantId the tenant owning the execution
     * @param executionId the execution identifier to update
     * @param expectedVersion the expected current version; the update is applied only if the stored version equals this value
     * @param finalStatus the terminal status to set; only `ExecutionStatus.FAILED` or `ExecutionStatus.DLQ` are accepted
     * @param transitionKey the transition key to record as the last transition
     * @param errorCode an error code to attach to the execution record
     * @param errorMessage an error message to attach; if longer than 512 characters it will be truncated
     * @param nowEpochMs current epoch milliseconds used for the record's updated timestamp
     * @return an Optional containing the updated ExecutionRecord when the status was applied (existing record found and version matched), or an empty Optional otherwise
     */
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
        if (finalStatus != ExecutionStatus.FAILED && finalStatus != ExecutionStatus.DLQ) {
            return Uni.createFrom().item(Optional.empty());
        }
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                String scopedId = scopedExecutionId(tenantId, executionId);
                ExecutionRecord<Object, Object> current = executionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion) {
                    return Optional.empty();
                }
                ExecutionRecord<Object, Object> updated = new ExecutionRecord<>(
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
     * Finds executions that are due to run, are not in a terminal state, have not expired by TTL,
     * and are currently lease-free, returning up to the specified limit.
     *
     * @param nowEpochMs the current time in milliseconds used to evaluate due times, lease expirations, and TTL
     * @param limit the maximum number of executions to return
     * @return an immutable list of executions that are due and lease-free, sorted by `nextDueEpochMs` ascending,
     *         containing at most `limit` entries
     */
    @Override
    public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
                List<ExecutionRecord<Object, Object>> due = new ArrayList<>();
                for (ExecutionRecord<Object, Object> record : executionsByScopedId.values()) {
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
     * Builds a deterministic composite identifier that scopes an execution to a tenant.
     *
     * @param tenantId    the tenant identifier
     * @param executionId the execution identifier
     * @return a composite scoped identifier combining the tenantId and executionId (formatted as lengths and values)
     */
    private static String scopedExecutionId(String tenantId, String executionId) {
        return compositeScopedKey("tenantId", tenantId, "executionId", executionId);
    }

    /**
     * Builds a deterministic, namespaced key that uniquely combines a tenant identifier and an execution key.
     *
     * @param tenantId    the tenant identifier
     * @param executionKey the execution-specific key
     * @return a composite scoped key combining `tenantId` and `executionKey`
     */
    private static String scopedExecutionKey(String tenantId, String executionKey) {
        return compositeScopedKey("tenantId", tenantId, "executionKey", executionKey);
    }

    /**
     * Builds a deterministic composite key from two string components.
     *
     * The returned key has the format "{left.length}:{left}:{right.length}:{right}".
     *
     * @param leftName  label used in the null-check message for the left component
     * @param left      the left string component (must not be null)
     * @param rightName label used in the null-check message for the right component
     * @param right     the right string component (must not be null)
     * @return          a composite key combining lengths and values of the two components
     * @throws NullPointerException if {@code left} or {@code right} is null
     */
    private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
        String safeLeft = Objects.requireNonNull(left, leftName + " must not be null");
        String safeRight = Objects.requireNonNull(right, rightName + " must not be null");
        return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
    }

    /**
     * Truncates a string to at most 512 characters.
     *
     * @param value the input string, may be null
     * @return the original string if its length is 512 characters or fewer, the input truncated to 512 characters if longer, or `null` if the input is `null`
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
