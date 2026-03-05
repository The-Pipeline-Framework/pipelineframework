package org.pipelineframework.orchestrator;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * DynamoDB-backed async execution state store.
 */
@ApplicationScoped
public class DynamoExecutionStateStore implements ExecutionStateStore {
    private static final Logger LOG = Logger.getLogger(DynamoExecutionStateStore.class);

    private static final String TENANT_ID = "tenant_id";
    private static final String EXECUTION_ID = "execution_id";
    private static final String EXECUTION_KEY = "execution_key";
    private static final String TENANT_EXECUTION_KEY = "tenant_execution_key";
    private static final String STATUS = "status";
    private static final String VERSION = "version";
    private static final String CURRENT_STEP_INDEX = "current_step_index";
    private static final String ATTEMPT = "attempt";
    private static final String LEASE_OWNER = "lease_owner";
    private static final String LEASE_EXPIRES_EPOCH_MS = "lease_expires_epoch_ms";
    private static final String NEXT_DUE_EPOCH_MS = "next_due_epoch_ms";
    private static final String LAST_TRANSITION_KEY = "last_transition_key";
    private static final String INPUT_SHAPE = "input_shape";
    private static final String INPUT_PAYLOAD_JSON = "input_payload_json";
    private static final String RESULT_PAYLOAD_JSON = "result_payload_json";
    private static final String ERROR_CODE = "error_code";
    private static final String ERROR_MESSAGE = "error_message";
    private static final String CREATED_AT_EPOCH_MS = "created_at_epoch_ms";
    private static final String UPDATED_AT_EPOCH_MS = "updated_at_epoch_ms";
    private static final String TTL_EPOCH_S = "ttl_epoch_s";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private volatile DynamoDbClient client;

    /**
     * Default constructor for CDI.
     */
    public DynamoExecutionStateStore() {
    }

    /**
     * Package-private constructor used for creating an instance with a preinitialized DynamoDbClient and configuration.
     *
     * @param client the DynamoDbClient to use for all DynamoDB operations (can be a test double)
     * @param orchestratorConfig configuration values for table names, region, and endpoint overrides
     */
    DynamoExecutionStateStore(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
    }

    /**
     * Identifies the storage provider name for this implementation.
     *
     * @return the provider name "dynamo"
     */
    @Override
    public String providerName() {
        return "dynamo";
    }

    @Override
    public int priority() {
        return -1000;
    }

    /**
     * Validates required DynamoDB configuration for the orchestrator.
     *
     * @param config the PipelineOrchestratorConfig to validate
     * @return an Optional containing a descriptive error message if validation fails, or an empty Optional if configuration is valid
     */
    @Override
    public Optional<String> startupValidationError(PipelineOrchestratorConfig config) {
        if (config == null || config.dynamo() == null) {
            return Optional.of("Dynamo provider requires pipeline.orchestrator.dynamo.* configuration.");
        }
        String executionTable = config.dynamo().executionTable();
        String keyTable = config.dynamo().executionKeyTable();
        if (executionTable == null || executionTable.isBlank()) {
            return Optional.of("pipeline.orchestrator.dynamo.execution-table must not be blank.");
        }
        if (keyTable == null || keyTable.isBlank()) {
            return Optional.of("pipeline.orchestrator.dynamo.execution-key-table must not be blank.");
        }
        return Optional.empty();
    }

    /**
     * Create a new execution from the provided command or retrieve an existing execution when a duplicate is detected.
     *
     * @param command the execution creation request containing tenant, execution key, input payload, and other creation parameters
     * @return a CreateExecutionResult containing the execution record and a flag indicating whether the execution already existed
     */
    @Override
    public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
        return blocking(() -> createOrGetExecutionBlocking(command));
    }

    /**
     * Retrieve the execution record for the given tenant and execution ID.
     *
     * @param tenantId    the tenant identifier that scopes the execution
     * @param executionId the execution identifier
     * @return            an Optional containing the execution record if present and not expired; {@code Optional.empty()} if not found or expired
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
        return blocking(() -> getExecutionBlocking(tenantId, executionId, System.currentTimeMillis()));
    }

    /**
     * Attempt to claim a lease for the specified execution and return the updated record when claiming succeeds.
     *
     * @param tenantId    the tenant identifier owning the execution
     * @param executionId the execution identifier to claim a lease for
     * @param leaseOwner  identifier of the entity attempting to own the lease
     * @param nowEpochMs  current time in milliseconds used for lease/TTL decisioning
     * @param leaseMs     desired lease duration in milliseconds
     * @return an Optional containing the updated ExecutionRecord if the lease was successfully claimed, or empty otherwise
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> claimLease(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs
    ) {
        return blocking(() -> claimLeaseBlocking(tenantId, executionId, leaseOwner, nowEpochMs, leaseMs));
    }

    /**
     * Mark an execution as succeeded and store the provided result payload.
     *
     * @param tenantId the tenant that owns the execution
     * @param executionId the execution identifier
     * @param expectedVersion the expected current record version; the update is applied only if this matches the stored version
     * @param transitionKey identifier for the transition that caused the success
     * @param resultPayload the result payload to persist with the succeeded execution (may be null)
     * @param nowEpochMs current time in milliseconds used for timestamps and TTL checks
     * @return an Optional containing the updated ExecutionRecord when the execution was transitioned to SUCCEEDED; `Optional.empty()` if the execution was not found or the conditional update failed (e.g., version mismatch)
     */
    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> markSucceeded(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs
    ) {
        return blocking(() -> markSucceededBlocking(
            tenantId,
            executionId,
            expectedVersion,
            transitionKey,
            resultPayload,
            nowEpochMs));
    }

    /**
     * Schedules a retry for the specified execution by updating its state to WAIT_RETRY with the provided
     * attempt number and next-due timestamp, and recording transition and error details.
     *
     * @param tenantId the tenant that owns the execution
     * @param executionId the id of the execution to update
     * @param expectedVersion the expected current version for a conditional update (used for concurrency control)
     * @param nextAttempt the next attempt number to set on the execution
     * @param nextDueEpochMs the epoch millisecond when the execution should next be retried
     * @param transitionKey a key describing the state transition
     * @param errorCode an error code to record with the retry
     * @param errorMessage an error message to record with the retry
     * @param nowEpochMs the current epoch millisecond used for TTL and expiry checks
     * @return `Optional` containing the updated ExecutionRecord if the conditional update succeeded, `Optional.empty()` otherwise
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
        long nowEpochMs
    ) {
        return blocking(() -> scheduleRetryBlocking(
            tenantId,
            executionId,
            expectedVersion,
            nextAttempt,
            nextDueEpochMs,
            transitionKey,
            errorCode,
            errorMessage,
            nowEpochMs));
    }

    /**
     * Attempts to mark the specified execution as a terminal failure and return the updated record.
     *
     * If `finalStatus` is not `FAILED` or `DLQ`, the method immediately returns `Optional.empty()`.
     *
     * @param tenantId the tenant owning the execution
     * @param executionId the execution identifier
     * @param expectedVersion the expected current version of the execution used for a conditional update
     * @param finalStatus the terminal status to apply (only `FAILED` or `DLQ` are accepted)
     * @param transitionKey a key describing the transition that led to the terminal state
     * @param errorCode an optional error code to record with the terminal transition
     * @param errorMessage an optional error message to record with the terminal transition
     * @param nowEpochMs current time in epoch milliseconds used for TTL/timestamp checks
     * @return an `Optional` containing the updated `ExecutionRecord` if the terminal transition was applied, `Optional.empty()` otherwise
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
        long nowEpochMs
    ) {
        if (finalStatus != ExecutionStatus.FAILED && finalStatus != ExecutionStatus.DLQ) {
            return Uni.createFrom().item(Optional.empty());
        }
        return blocking(() -> markTerminalFailureBlocking(
            tenantId,
            executionId,
            expectedVersion,
            finalStatus,
            transitionKey,
            errorCode,
            errorMessage,
            nowEpochMs));
    }

    /**
     * Finds executions that are due to run at or before the given timestamp.
     *
     * @param nowEpochMs current time in epoch milliseconds used to evaluate which executions are due
     * @param limit maximum number of executions to return; values less than or equal to 0 produce an empty list
     * @return a list of execution records due at or before {@code nowEpochMs}, ordered by next-due time ascending and limited to at most {@code limit} entries
     */
    @Override
    public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
        if (limit <= 0) {
            return Uni.createFrom().item(List.of());
        }
        return blocking(() -> findDueExecutionsBlocking(nowEpochMs, limit));
    }

    /**
     * Releases and closes the lazily-initialized DynamoDbClient if present, performing the operation
     * in a thread-safe, idempotent manner.
     *
     * <p>The method attempts to close the client and clears the internal reference. Any exception
     * thrown while closing is suppressed (logged at debug level) and does not propagate.
     */
    @PreDestroy
    void closeClient() {
        DynamoDbClient active = client;
        if (active == null) {
            return;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing DynamoDB client during shutdown.", e);
            } finally {
                client = null;
            }
        }
    }

    /**
     * Create a new execution for the supplied create command or return an existing execution
     * if a scoped deduplication key already maps to a stored execution.
     *
     * @param command the execution creation command containing tenantId, executionKey, timestamps, input payload, and TTL
     * @return a CreateExecutionResult containing the execution record and a flag that is `true` if the execution already existed, `false` if a new execution was created
     */
    private CreateExecutionResult createOrGetExecutionBlocking(ExecutionCreateCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        long nowEpochMs = command.nowEpochMs();
        String scopedExecutionKey = scopedExecutionKey(command.tenantId(), command.executionKey());
        Optional<ExecutionRecord<Object, Object>> existing = findExistingByScopedExecutionKey(
            command.tenantId(),
            scopedExecutionKey,
            nowEpochMs);
        if (existing.isPresent()) {
            return new CreateExecutionResult(existing.get(), true);
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

        try {
            writeNewExecution(scopedExecutionKey, created, command.nowEpochMs(), command.ttlEpochS());
            return new CreateExecutionResult(created, false);
        } catch (TransactionCanceledException | ConditionalCheckFailedException ignored) {
            Optional<ExecutionRecord<Object, Object>> raced = findExistingByScopedExecutionKey(
                command.tenantId(),
                scopedExecutionKey,
                nowEpochMs);
            if (raced.isPresent()) {
                return new CreateExecutionResult(raced.get(), true);
            }
            throw ignored;
        }
    }

    /**
     * Retrieves the execution for the given tenant and execution ID, returning it if present and not expired; expired records are removed as a side effect.
     *
     * @param tenantId the tenant identifier
     * @param executionId the execution identifier
     * @param nowEpochMs current time in milliseconds used to evaluate TTL/expiration
     * @return an Optional containing the execution record if found and not expired, or empty otherwise
     */
    private Optional<ExecutionRecord<Object, Object>> getExecutionBlocking(String tenantId, String executionId, long nowEpochMs) {
        GetItemRequest request = GetItemRequest.builder()
            .tableName(executionTable())
            .key(executionPrimaryKey(tenantId, executionId))
            .consistentRead(true)
            .build();
        Map<String, AttributeValue> item = dynamoClient().getItem(request).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        ExecutionRecord<Object, Object> record = toRecord(item);
        if (!isExpired(record, nowEpochMs)) {
            return Optional.of(record);
        }
        deleteExpiredRecord(record);
        return Optional.empty();
    }

    /**
     * Attempts to claim a lease for the specified execution and returns the updated execution record when the claim succeeds.
     *
     * The claim only succeeds if the execution is due (nextDue <= now), there is no active lease or the existing lease has expired,
     * the execution is not in a terminal status (SUCCEEDED, FAILED, DLQ), and the record's TTL is not expired.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the execution identifier
     * @param leaseOwner the identifier for the lease owner to set when claiming the lease
     * @param nowEpochMs the current time in milliseconds since epoch used for evaluating due/expiry conditions
     * @param leaseMs the lease duration in milliseconds to apply when the claim succeeds
     * @return an Optional containing the updated ExecutionRecord with the claimed lease if the claim succeeded, or an empty Optional if the claim failed or conditions were not met
     */
    private Optional<ExecutionRecord<Object, Object>> claimLeaseBlocking(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs
    ) {
        Map<String, String> names = Map.of(
            "#status", STATUS,
            "#nextDue", NEXT_DUE_EPOCH_MS,
            "#leaseOwner", LEASE_OWNER,
            "#leaseExpires", LEASE_EXPIRES_EPOCH_MS,
            "#version", VERSION,
            "#updated", UPDATED_AT_EPOCH_MS,
            "#ttl", TTL_EPOCH_S);
        Map<String, AttributeValue> values = Map.of(
            ":now", avN(nowEpochMs),
            ":leaseOwner", avS(leaseOwner),
            ":leaseExpires", avN(nowEpochMs + leaseMs),
            ":running", avS(ExecutionStatus.RUNNING.name()),
            ":one", avN(1),
            ":succeeded", avS(ExecutionStatus.SUCCEEDED.name()),
            ":failed", avS(ExecutionStatus.FAILED.name()),
            ":dlq", avS(ExecutionStatus.DLQ.name()),
            ":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));

        UpdateItemRequest request = UpdateItemRequest.builder()
            .tableName(executionTable())
            .key(executionPrimaryKey(tenantId, executionId))
            .conditionExpression(
                "#nextDue <= :now " +
                    "AND (attribute_not_exists(#leaseOwner) OR #leaseExpires <= :now) " +
                    "AND #status <> :succeeded AND #status <> :failed AND #status <> :dlq " +
                    "AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
            .updateExpression(
                "SET #status = :running, #leaseOwner = :leaseOwner, #leaseExpires = :leaseExpires, " +
                    "#updated = :now, #version = #version + :one")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .returnValues(ReturnValue.ALL_NEW)
            .build();
        try {
            Map<String, AttributeValue> attributes = dynamoClient().updateItem(request).attributes();
            if (attributes == null || attributes.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(toRecord(attributes));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        }
    }

    /**
     * Mark the specified execution as succeeded and update its terminal metadata.
     *
     * Updates the execution's status to SUCCEEDED, stores the provided transition key and result
     * payload, clears lease and error fields, increments the version, and updates next-due/updated
     * timestamps. The update is applied only if the current version equals {@code expectedVersion}
     * and the item's TTL (if present) is still in the future.
     *
     * @param tenantId the tenant owning the execution
     * @param executionId the execution identifier
     * @param expectedVersion the expected current version for a conditional update
     * @param transitionKey a transition key to record (may be null)
     * @param resultPayload the result payload to store (may be null)
     * @param nowEpochMs current time in milliseconds used to set timestamps and TTL checks
     * @return an {@link Optional} containing the updated {@code ExecutionRecord} when the conditional
     *         update succeeds, or an empty {@code Optional} if the condition fails or the item is
     *         missing/expired
     */
    private Optional<ExecutionRecord<Object, Object>> markSucceededBlocking(
        String tenantId,
        String executionId,
        long expectedVersion,
        String transitionKey,
        Object resultPayload,
        long nowEpochMs
    ) {
        Map<String, String> names = new HashMap<>();
        names.put("#status", STATUS);
        names.put("#version", VERSION);
        names.put("#transition", LAST_TRANSITION_KEY);
        names.put("#result", RESULT_PAYLOAD_JSON);
        names.put("#errorCode", ERROR_CODE);
        names.put("#errorMessage", ERROR_MESSAGE);
        names.put("#leaseOwner", LEASE_OWNER);
        names.put("#leaseExpires", LEASE_EXPIRES_EPOCH_MS);
        names.put("#nextDue", NEXT_DUE_EPOCH_MS);
        names.put("#updated", UPDATED_AT_EPOCH_MS);
        names.put("#ttl", TTL_EPOCH_S);

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":expected", avN(expectedVersion));
        values.put(":succeeded", avS(ExecutionStatus.SUCCEEDED.name()));
        values.put(":transition", avS(transitionKey == null ? "" : transitionKey));
        values.put(":result", avS(toJson(resultPayload)));
        values.put(":zero", avN(0));
        values.put(":now", avN(nowEpochMs));
        values.put(":one", avN(1));
        values.put(":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));

        UpdateItemRequest request = UpdateItemRequest.builder()
            .tableName(executionTable())
            .key(executionPrimaryKey(tenantId, executionId))
            .conditionExpression("#version = :expected AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
            .updateExpression(
                "SET #status = :succeeded, #version = #version + :one, #transition = :transition, " +
                    "#result = :result, #leaseExpires = :zero, #nextDue = :now, #updated = :now " +
                    "REMOVE #errorCode, #errorMessage, #leaseOwner")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .returnValues(ReturnValue.ALL_NEW)
            .build();
        try {
            Map<String, AttributeValue> attributes = dynamoClient().updateItem(request).attributes();
            if (attributes == null || attributes.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(toRecord(attributes));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        }
    }

    /**
     * Atomically updates an execution to the WAIT_RETRY state with new attempt and next-due time,
     * clears lease fields, records transition and error details, and returns the updated record when successful.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the execution identifier to update
     * @param expectedVersion the expected current version used for conditional update
     * @param nextAttempt the attempt count to set on the execution
     * @param nextDueEpochMs the epoch millisecond timestamp when the execution should next be processed
     * @param transitionKey an optional transition key to record (may be null)
     * @param errorCode an optional error code to record (may be null)
     * @param errorMessage an optional error message to record (may be null)
     * @param nowEpochMs the current epoch millisecond timestamp used to set the updated timestamp and TTL checks
     * @return an Optional containing the updated ExecutionRecord if the conditional update succeeds; Optional.empty() if the condition fails or the item does not exist
     */
    private Optional<ExecutionRecord<Object, Object>> scheduleRetryBlocking(
        String tenantId,
        String executionId,
        long expectedVersion,
        int nextAttempt,
        long nextDueEpochMs,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs
    ) {
        Map<String, String> names = Map.ofEntries(
            Map.entry("#status", STATUS),
            Map.entry("#version", VERSION),
            Map.entry("#attempt", ATTEMPT),
            Map.entry("#nextDue", NEXT_DUE_EPOCH_MS),
            Map.entry("#transition", LAST_TRANSITION_KEY),
            Map.entry("#errorCode", ERROR_CODE),
            Map.entry("#errorMessage", ERROR_MESSAGE),
            Map.entry("#result", RESULT_PAYLOAD_JSON),
            Map.entry("#leaseOwner", LEASE_OWNER),
            Map.entry("#leaseExpires", LEASE_EXPIRES_EPOCH_MS),
            Map.entry("#updated", UPDATED_AT_EPOCH_MS),
            Map.entry("#ttl", TTL_EPOCH_S));
        Map<String, AttributeValue> values = Map.ofEntries(
            Map.entry(":expected", avN(expectedVersion)),
            Map.entry(":retry", avS(ExecutionStatus.WAIT_RETRY.name())),
            Map.entry(":attempt", avN(nextAttempt)),
            Map.entry(":nextDue", avN(nextDueEpochMs)),
            Map.entry(":transition", avS(transitionKey == null ? "" : transitionKey)),
            Map.entry(":errorCode", avS(errorCode == null ? "" : errorCode)),
            Map.entry(":errorMessage", avS(truncate(errorMessage))),
            Map.entry(":zero", avN(0)),
            Map.entry(":now", avN(nowEpochMs)),
            Map.entry(":one", avN(1)),
            Map.entry(":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond())));

        UpdateItemRequest request = UpdateItemRequest.builder()
            .tableName(executionTable())
            .key(executionPrimaryKey(tenantId, executionId))
            .conditionExpression("#version = :expected AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
            .updateExpression(
                "SET #status = :retry, #version = #version + :one, #attempt = :attempt, #nextDue = :nextDue, " +
                    "#transition = :transition, #errorCode = :errorCode, #errorMessage = :errorMessage, " +
                    "#leaseExpires = :zero, #updated = :now REMOVE #result, #leaseOwner")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .returnValues(ReturnValue.ALL_NEW)
            .build();
        try {
            Map<String, AttributeValue> attributes = dynamoClient().updateItem(request).attributes();
            if (attributes == null || attributes.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(toRecord(attributes));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        }
    }

    /**
     * Set the execution to the given terminal failure status and return the updated record when the conditional update succeeds.
     *
     * The update is applied only if the stored version equals `expectedVersion` and the item's TTL (if present) is in the future.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param executionId the execution identifier to update
     * @param expectedVersion the expected current version of the execution; the update is conditional on this matching
     * @param finalStatus the terminal status to set (e.g., FAILED or DLQ)
     * @param transitionKey an optional transition key to record; may be null
     * @param errorCode an optional error code to record; may be null
     * @param errorMessage an optional error message to record; may be null and will be truncated if excessively long
     * @param nowEpochMs the current time in milliseconds used to update timestamps and compute TTL checks
     * @return an Optional containing the updated ExecutionRecord if the conditional update succeeded, or empty if the condition failed or the item was not updated
     */
    private Optional<ExecutionRecord<Object, Object>> markTerminalFailureBlocking(
        String tenantId,
        String executionId,
        long expectedVersion,
        ExecutionStatus finalStatus,
        String transitionKey,
        String errorCode,
        String errorMessage,
        long nowEpochMs
    ) {
        Map<String, String> names = Map.ofEntries(
            Map.entry("#status", STATUS),
            Map.entry("#version", VERSION),
            Map.entry("#nextDue", NEXT_DUE_EPOCH_MS),
            Map.entry("#transition", LAST_TRANSITION_KEY),
            Map.entry("#errorCode", ERROR_CODE),
            Map.entry("#errorMessage", ERROR_MESSAGE),
            Map.entry("#result", RESULT_PAYLOAD_JSON),
            Map.entry("#leaseOwner", LEASE_OWNER),
            Map.entry("#leaseExpires", LEASE_EXPIRES_EPOCH_MS),
            Map.entry("#updated", UPDATED_AT_EPOCH_MS),
            Map.entry("#ttl", TTL_EPOCH_S));
        Map<String, AttributeValue> values = Map.of(
            ":expected", avN(expectedVersion),
            ":finalStatus", avS(finalStatus.name()),
            ":transition", avS(transitionKey == null ? "" : transitionKey),
            ":errorCode", avS(errorCode == null ? "" : errorCode),
            ":errorMessage", avS(truncate(errorMessage)),
            ":zero", avN(0),
            ":now", avN(nowEpochMs),
            ":one", avN(1),
            ":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));

        UpdateItemRequest request = UpdateItemRequest.builder()
            .tableName(executionTable())
            .key(executionPrimaryKey(tenantId, executionId))
            .conditionExpression("#version = :expected AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
            .updateExpression(
                "SET #status = :finalStatus, #version = #version + :one, #nextDue = :now, #transition = :transition, " +
                    "#errorCode = :errorCode, #errorMessage = :errorMessage, #leaseExpires = :zero, #updated = :now " +
                    "REMOVE #result, #leaseOwner")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .returnValues(ReturnValue.ALL_NEW)
            .build();
        try {
            Map<String, AttributeValue> attributes = dynamoClient().updateItem(request).attributes();
            if (attributes == null || attributes.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(toRecord(attributes));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        }
    }

    /**
     * Finds executions that are due at or before the provided epoch millisecond timestamp and returns up to the specified limit.
     *
     * The returned candidates exclude executions that are expired (by TTL), in terminal statuses (SUCCEEDED, FAILED, DLQ),
     * or currently leased; results are sorted by next due time ascending.
     *
     * @param nowEpochMs epoch millisecond instant used to evaluate due time, lease expiry, and TTL
     * @param limit maximum number of execution records to return
     * @return a list of execution records sorted by next due time (ascending), containing at most {@code limit} entries; empty list if none found
     */
    private List<ExecutionRecord<Object, Object>> findDueExecutionsBlocking(long nowEpochMs, int limit) {
        Map<String, String> names = Map.of(
            "#status", STATUS,
            "#nextDue", NEXT_DUE_EPOCH_MS,
            "#leaseOwner", LEASE_OWNER,
            "#leaseExpires", LEASE_EXPIRES_EPOCH_MS,
            "#ttl", TTL_EPOCH_S);
        Map<String, AttributeValue> values = Map.of(
            ":now", avN(nowEpochMs),
            ":succeeded", avS(ExecutionStatus.SUCCEEDED.name()),
            ":failed", avS(ExecutionStatus.FAILED.name()),
            ":dlq", avS(ExecutionStatus.DLQ.name()),
            ":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));

        int candidateLimit = Math.max(limit * 3, limit);
        List<ExecutionRecord<Object, Object>> due = new ArrayList<>();

        Map<String, AttributeValue> exclusiveStartKey = null;
        while (true) {
            ScanRequest.Builder requestBuilder = ScanRequest.builder()
                .tableName(executionTable())
                .filterExpression(
                    "#nextDue <= :now " +
                        "AND (attribute_not_exists(#leaseOwner) OR #leaseExpires <= :now) " +
                        "AND #status <> :succeeded AND #status <> :failed AND #status <> :dlq " +
                        "AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .limit(candidateLimit);
            if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
                requestBuilder.exclusiveStartKey(exclusiveStartKey);
            }

            ScanResponse response = dynamoClient().scan(requestBuilder.build());
            if (response.items() != null) {
                for (Map<String, AttributeValue> item : response.items()) {
                    ExecutionRecord<Object, Object> record = toRecord(item);
                    if (!isExpired(record, nowEpochMs)) {
                        due.add(record);
                    }
                }
            }

            if (due.size() >= candidateLimit || response.lastEvaluatedKey() == null || response.lastEvaluatedKey().isEmpty()) {
                break;
            }
            exclusiveStartKey = response.lastEvaluatedKey();
        }

        if (due.isEmpty()) {
            return List.of();
        }
        due.sort(Comparator.comparingLong(ExecutionRecord::nextDueEpochMs));
        if (due.size() > limit) {
            return List.copyOf(due.subList(0, limit));
        }
        return List.copyOf(due);
    }

    /**
     * Looks up an execution by its scoped execution key and returns the corresponding execution record if present and not expired.
     *
     * @param tenantId the tenant identifier owning the execution
     * @param scopedExecutionKey the deduplication key used to locate the execution in the execution-key table
     * @param nowEpochMs current time in epoch milliseconds used to evaluate TTL/expiry
     * @return an Optional containing the execution record when found and not expired; otherwise an empty Optional.
     *         If a key entry is found but the referenced execution is missing or expired, the key entry is deleted as a best-effort cleanup.
     */
    private Optional<ExecutionRecord<Object, Object>> findExistingByScopedExecutionKey(
        String tenantId,
        String scopedExecutionKey,
        long nowEpochMs
    ) {
        GetItemRequest keyRequest = GetItemRequest.builder()
            .tableName(executionKeyTable())
            .key(Map.of(TENANT_EXECUTION_KEY, avS(scopedExecutionKey)))
            .consistentRead(true)
            .build();
        Map<String, AttributeValue> keyItem = dynamoClient().getItem(keyRequest).item();
        if (keyItem == null || keyItem.isEmpty()) {
            return Optional.empty();
        }
        String executionId = readString(keyItem, EXECUTION_ID);
        if (executionId == null || executionId.isBlank()) {
            return Optional.empty();
        }
        Optional<ExecutionRecord<Object, Object>> existing = getExecutionBlocking(tenantId, executionId, nowEpochMs);
        if (existing.isPresent()) {
            return existing;
        }
        deleteExecutionKey(scopedExecutionKey);
        return Optional.empty();
    }

    /**
     * Persists a new execution and its deduplication key atomically to DynamoDB.
     *
     * Writes an execution item to the configured execution table and a corresponding
     * scoped key item to the execution-key table in a single transaction, ensuring
     * neither item previously exists. The key item includes tenant, execution id,
     * creation/update timestamps (milliseconds) and a TTL (seconds).
     *
     * @param scopedExecutionKey the tenant-scoped deduplication key for this execution
     * @param record the execution record to persist
     * @param nowEpochMs the creation/update timestamp in milliseconds to store on both items
     * @param ttlEpochS the TTL epoch time in seconds to set for the persisted items
     */
    private void writeNewExecution(
        String scopedExecutionKey,
        ExecutionRecord<Object, Object> record,
        long nowEpochMs,
        long ttlEpochS
    ) {
        Map<String, AttributeValue> executionItem = toItem(record);
        Map<String, AttributeValue> keyItem = new HashMap<>();
        keyItem.put(TENANT_EXECUTION_KEY, avS(scopedExecutionKey));
        keyItem.put(TENANT_ID, avS(record.tenantId()));
        keyItem.put(EXECUTION_ID, avS(record.executionId()));
        keyItem.put(CREATED_AT_EPOCH_MS, avN(nowEpochMs));
        keyItem.put(UPDATED_AT_EPOCH_MS, avN(nowEpochMs));
        keyItem.put(TTL_EPOCH_S, avN(ttlEpochS));

        Put putExecution = Put.builder()
            .tableName(executionTable())
            .item(executionItem)
            .conditionExpression("attribute_not_exists(#tenantId) AND attribute_not_exists(#executionId)")
            .expressionAttributeNames(Map.of("#tenantId", TENANT_ID, "#executionId", EXECUTION_ID))
            .build();

        Put putKey = Put.builder()
            .tableName(executionKeyTable())
            .item(keyItem)
            .conditionExpression("attribute_not_exists(#scopedExecutionKey)")
            .expressionAttributeNames(Map.of("#scopedExecutionKey", TENANT_EXECUTION_KEY))
            .build();

        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(
                TransactWriteItem.builder().put(putExecution).build(),
                TransactWriteItem.builder().put(putKey).build())
            .build();
        dynamoClient().transactWriteItems(request);
    }

    /**
     * Attempts best-effort deletion of the DynamoDB execution item and its scoped key for the given record.
     *
     * Deletes the execution item from the configured execution table and then attempts to remove the associated
     * scoped execution key; failures while deleting the execution item are ignored.
     *
     * @param record the execution record whose DynamoDB entries should be removed
     */
    private void deleteExpiredRecord(ExecutionRecord<Object, Object> record) {
        try {
            dynamoClient().deleteItem(DeleteItemRequest.builder()
                .tableName(executionTable())
                .key(executionPrimaryKey(record.tenantId(), record.executionId()))
                .build());
        } catch (Exception ignored) {
            // Best-effort cleanup for expired items.
        }
        deleteExecutionKey(scopedExecutionKey(record.tenantId(), record.executionKey()));
    }

    /**
     * Removes the deduplication entry for the given scoped execution key from the execution-key table.
     *
     * This is a best-effort cleanup; failures are ignored and do not propagate.
     *
     * @param scopedExecutionKey the tenant-scoped deduplication key to delete
     */
    private void deleteExecutionKey(String scopedExecutionKey) {
        try {
            dynamoClient().deleteItem(DeleteItemRequest.builder()
                .tableName(executionKeyTable())
                .key(Map.of(TENANT_EXECUTION_KEY, avS(scopedExecutionKey)))
                .build());
        } catch (Exception ignored) {
            // Best-effort cleanup for stale dedup keys.
        }
    }

    /**
     * Convert an ExecutionRecord into a DynamoDB item map suitable for persistence.
     *
     * <p>The resulting map contains the record's primary and metadata attributes (tenant, execution id,
     * execution key, status, version, step/attempt/lease/timing fields, created/updated timestamps and TTL).
     * Optional fields—lease owner, last transition key, input payload (with optional shape), result payload JSON,
     * error code, and error message—are included only when present on the record.
     *
     * @param record the execution record to serialize into a DynamoDB attribute map
     * @return a map of DynamoDB attribute names to AttributeValue representing the execution record
     */
    private Map<String, AttributeValue> toItem(ExecutionRecord<Object, Object> record) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TENANT_ID, avS(record.tenantId()));
        item.put(EXECUTION_ID, avS(record.executionId()));
        item.put(EXECUTION_KEY, avS(record.executionKey()));
        item.put(STATUS, avS(record.status().name()));
        item.put(VERSION, avN(record.version()));
        item.put(CURRENT_STEP_INDEX, avN(record.currentStepIndex()));
        item.put(ATTEMPT, avN(record.attempt()));
        item.put(LEASE_EXPIRES_EPOCH_MS, avN(record.leaseExpiresEpochMs()));
        item.put(NEXT_DUE_EPOCH_MS, avN(record.nextDueEpochMs()));
        item.put(CREATED_AT_EPOCH_MS, avN(record.createdAtEpochMs()));
        item.put(UPDATED_AT_EPOCH_MS, avN(record.updatedAtEpochMs()));
        item.put(TTL_EPOCH_S, avN(record.ttlEpochS()));
        putIfPresent(item, LEASE_OWNER, record.leaseOwner());
        putIfPresent(item, LAST_TRANSITION_KEY, record.lastTransitionKey());
        putInputPayload(item, record.inputPayload());
        putIfPresent(item, RESULT_PAYLOAD_JSON, toJson(record.resultPayload()));
        putIfPresent(item, ERROR_CODE, record.errorCode());
        putIfPresent(item, ERROR_MESSAGE, record.errorMessage());
        return item;
    }

    /**
     * Adds the input payload to the provided DynamoDB item map.
     *
     * If `inputPayload` is null this method does nothing. If `inputPayload` is an
     * ExecutionInputSnapshot, the snapshot's shape name is stored under the
     * `INPUT_SHAPE` attribute and the snapshot payload is serialized and stored
     * under `INPUT_PAYLOAD_JSON`. For any other non-null value the value is
     * serialized and stored under `INPUT_PAYLOAD_JSON`.
     *
     * @param item         the DynamoDB item attribute map to modify
     * @param inputPayload the execution input value or an ExecutionInputSnapshot containing shape metadata
     */
    private void putInputPayload(Map<String, AttributeValue> item, Object inputPayload) {
        if (inputPayload == null) {
            return;
        }
        if (inputPayload instanceof ExecutionInputSnapshot snapshot) {
            item.put(INPUT_SHAPE, avS(snapshot.shape().name()));
            putIfPresent(item, INPUT_PAYLOAD_JSON, toJson(snapshot.payload()));
            return;
        }
        putIfPresent(item, INPUT_PAYLOAD_JSON, toJson(inputPayload));
    }

    /**
     * Adds a string attribute to a DynamoDB item map when the provided value is not null or blank.
     *
     * @param item  the DynamoDB item map to modify
     * @param key   the attribute name to set on the item
     * @param value the string value to store; ignored if null or blank
     */
    private static void putIfPresent(Map<String, AttributeValue> item, String key, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        item.put(key, avS(value));
    }

    /**
     * Convert a DynamoDB item map into an ExecutionRecord populated with status, payloads, timestamps, and metadata.
     *
     * @return the ExecutionRecord built from the provided DynamoDB item map
     */
    private ExecutionRecord<Object, Object> toRecord(Map<String, AttributeValue> item) {
        String tenantId = readString(item, TENANT_ID);
        String executionId = readString(item, EXECUTION_ID);
        String executionKey = readString(item, EXECUTION_KEY);
        ExecutionStatus status = ExecutionStatus.valueOf(readString(item, STATUS));
        long version = readLong(item, VERSION);
        int currentStepIndex = (int) readLong(item, CURRENT_STEP_INDEX);
        int attempt = (int) readLong(item, ATTEMPT);
        String leaseOwner = readString(item, LEASE_OWNER);
        long leaseExpires = readLong(item, LEASE_EXPIRES_EPOCH_MS);
        long nextDue = readLong(item, NEXT_DUE_EPOCH_MS);
        String transitionKey = readString(item, LAST_TRANSITION_KEY);
        Object inputPayload = readInputPayload(item);
        Object resultPayload = readPayload(item.get(RESULT_PAYLOAD_JSON));
        String errorCode = readString(item, ERROR_CODE);
        String errorMessage = readString(item, ERROR_MESSAGE);
        long createdAt = readLong(item, CREATED_AT_EPOCH_MS);
        long updatedAt = readLong(item, UPDATED_AT_EPOCH_MS);
        long ttlEpochS = readLong(item, TTL_EPOCH_S);
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionKey,
            status,
            version,
            currentStepIndex,
            attempt,
            leaseOwner,
            leaseExpires,
            nextDue,
            transitionKey,
            inputPayload,
            resultPayload,
            errorCode,
            errorMessage,
            createdAt,
            updatedAt,
            ttlEpochS);
    }

    /**
     * Reads and deserializes the input payload (and optional input shape) from a DynamoDB item.
     *
     * @param item the DynamoDB item map containing input payload and optional shape attributes
     * @return `null` if no payload is present; the deserialized payload object otherwise. If an input
     *     shape attribute is present and matches a known ExecutionInputShape, returns an
     *     ExecutionInputSnapshot that wraps the payload; if the shape attribute is unrecognized,
     *     returns the deserialized payload object.
     */
    private Object readInputPayload(Map<String, AttributeValue> item) {
        AttributeValue payloadValue = item.get(INPUT_PAYLOAD_JSON);
        if (payloadValue == null || payloadValue.s() == null || payloadValue.s().isBlank()) {
            return null;
        }
        Object payload = fromJson(payloadValue.s());
        String shapeValue = readString(item, INPUT_SHAPE);
        if (shapeValue == null || shapeValue.isBlank()) {
            return payload;
        }
        try {
            ExecutionInputShape shape = ExecutionInputShape.valueOf(shapeValue);
            return new ExecutionInputSnapshot(shape, payload);
        } catch (IllegalArgumentException ignored) {
            return payload;
        }
    }

    /**
     * Deserialize a JSON payload stored in a DynamoDB AttributeValue.
     *
     * If the attribute is null or contains a null/blank string, this returns `null`.
     *
     * @param value the DynamoDB AttributeValue containing a JSON string
     * @return the deserialized Object, or `null` if the attribute is absent or blank
     */
    private Object readPayload(AttributeValue value) {
        if (value == null || value.s() == null || value.s().isBlank()) {
            return null;
        }
        return fromJson(value.s());
    }

    /**
     * Get the DynamoDB execution table name from the orchestrator configuration.
     *
     * @return the configured execution table name
     */
    private String executionTable() {
        return orchestratorConfig.dynamo().executionTable();
    }

    /**
     * Retrieve the configured DynamoDB table name used for mapping scoped execution keys to executions.
     *
     * @return the execution-key table name from configuration
     */
    private String executionKeyTable() {
        return orchestratorConfig.dynamo().executionKeyTable();
    }

    /**
     * Builds the DynamoDB primary key map for an execution item.
     *
     * @param tenantId    the tenant identifier to use as the TENANT_ID attribute
     * @param executionId the execution identifier to use as the EXECUTION_ID attribute
     * @return a map mapping TENANT_ID and EXECUTION_ID to string AttributeValue instances suitable for DynamoDB requests
     */
    private static Map<String, AttributeValue> executionPrimaryKey(String tenantId, String executionId) {
        return Map.of(
            TENANT_ID, avS(tenantId),
            EXECUTION_ID, avS(executionId));
    }

    /**
     * Determines whether an execution record is expired based on its TTL and a reference time.
     *
     * If the record's TTL is less than or equal to the provided reference time (in seconds), the
     * record is considered expired. A non-positive TTL is treated as "no TTL" and is never expired.
     *
     * @param record     the execution record to check
     * @param nowEpochMs the reference time in milliseconds since the epoch
     * @return `true` if the record's TTL is less than or equal to the reference time, `false` otherwise
     */
    private static boolean isExpired(ExecutionRecord<Object, Object> record, long nowEpochMs) {
        if (record.ttlEpochS() <= 0) {
            return false;
        }
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        return record.ttlEpochS() <= nowEpochS;
    }

    /**
     * Create a DynamoDB string AttributeValue from the given string.
     *
     * @param value the string to store in the AttributeValue
     * @return an AttributeValue representing the provided string
     */
    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    /**
     * Create a DynamoDB numeric AttributeValue from the given long.
     *
     * @param value the numeric value to convert
     * @return an AttributeValue representing the numeric value
     */
    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    /**
     * Read a string attribute from a DynamoDB item map.
     *
     * @param item the DynamoDB item map of attribute names to AttributeValue
     * @param key  the attribute name to read
     * @return     `null` if the attribute is absent, otherwise the attribute's string value
     */
    private static String readString(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null) {
            return null;
        }
        return value.s();
    }

    /**
     * Convert a numeric DynamoDB attribute value to a primitive long.
     *
     * @param item the DynamoDB item map containing attribute values
     * @param key the attribute key to read from the item
     * @return `0` if the attribute is missing or empty; otherwise the attribute parsed as a `long`
     */
    private static long readLong(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null || value.n() == null || value.n().isBlank()) {
            return 0L;
        }
        return Long.parseLong(value.n());
    }

    /**
     * Constructs a tenant-scoped execution key used for deduplication and lookups.
     *
     * @param tenantId     the tenant identifier (must not be null)
     * @param executionKey the execution-level key (must not be null)
     * @return             a scoped key string in the form "tenantLength:tenant:executionKeyLength:executionKey"
     * @throws NullPointerException if {@code tenantId} or {@code executionKey} is null
     */
    private static String scopedExecutionKey(String tenantId, String executionKey) {
        String safeTenant = Objects.requireNonNull(tenantId, "tenantId must not be null");
        String safeKey = Objects.requireNonNull(executionKey, "executionKey must not be null");
        return safeTenant.length() + ":" + safeTenant + ":" + safeKey.length() + ":" + safeKey;
    }

    /**
     * Truncates a string to at most 512 characters.
     *
     * @param value the input string, may be {@code null}
     * @return {@code null} if {@code value} is {@code null}; otherwise the original string truncated to at most 512 characters
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

    /**
     * Serialize an object to a JSON string using the pipeline JSON mapper.
     *
     * @param value the object to serialize; may be null
     * @return the JSON string representation of {@code value}, or {@code null} if {@code value} is null
     * @throws IllegalStateException if serialization fails
     */
    private static String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return PipelineJson.mapper().writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing execution payload to JSON.", e);
        }
    }

    /**
     * Deserialize a JSON string into a Java object.
     *
     * @param value JSON text to deserialize; may be null or blank
     * @return the deserialized Object, or `null` if the input is null or blank
     * @throws IllegalStateException if deserialization fails
     */
    private static Object fromJson(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return PipelineJson.mapper().readValue(value, Object.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed deserializing execution payload JSON.", e);
        }
    }

    /**
     * Provides the shared DynamoDbClient, initializing and caching it on first use.
     *
     * The client is created once and reused for subsequent calls; initialization applies the
     * optional region and endpoint override from the orchestrator configuration when present.
     *
     * @return the initialized and cached {@link DynamoDbClient} instance
     */
    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = DynamoDbClient.builder();
                orchestratorConfig.dynamo().region()
                    .filter(region -> !region.isBlank())
                    .ifPresent(region -> builder.region(Region.of(region)));
                orchestratorConfig.dynamo().endpointOverride()
                    .filter(endpoint -> !endpoint.isBlank())
                    .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                client = builder.build();
            }
            return client;
        }
    }

    /**
     * Run a blocking computation on the default Mutiny executor and expose its result as a Uni.
     *
     * @param <T> the type of the supplier result
     * @param supplier the blocking computation to execute
     * @return the supplier's result wrapped in a Uni
     */
    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }
}
