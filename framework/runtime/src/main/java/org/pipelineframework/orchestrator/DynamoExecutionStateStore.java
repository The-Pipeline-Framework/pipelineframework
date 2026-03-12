package org.pipelineframework.orchestrator;

import java.net.URI;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.google.protobuf.Message;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.ProtobufMessageParser;
import org.pipelineframework.config.pipeline.PipelineJson;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
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
    private static final String ENCODED_TYPE = "_tpf_type";
    private static final String ENCODED_MESSAGE_CLASS = "protobuf";
    private static final String ENCODED_MESSAGE_NAME = "_tpf_message";
    private static final String ENCODED_MESSAGE_JAVA_CLASS = "_tpf_java_class";
    private static final String ENCODED_PAYLOAD = "_tpf_payload_b64";
    private static final String ENCODED_INTERNAL = "_tpf_internal";
    private static final String ENCODED_ESCAPED_MAP = "_tpf_user_map";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    Instance<ProtobufMessageParser> protobufMessageParsers;

    private volatile Map<String, ProtobufMessageParser> protobufParserLookup;
    private volatile DynamoDbClient client;

    /**
     * Default constructor for CDI.
     */
    public DynamoExecutionStateStore() {
    }

    DynamoExecutionStateStore(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this(client, orchestratorConfig, null);
    }

    DynamoExecutionStateStore(
        DynamoDbClient client,
        PipelineOrchestratorConfig orchestratorConfig,
        Instance<ProtobufMessageParser> protobufMessageParsers
    ) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
        this.protobufMessageParsers = protobufMessageParsers;
    }

    @Override
    public String providerName() {
        return "dynamo";
    }

    @Override
    public int priority() {
        return -1000;
    }

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

    @Override
    public Uni<CreateExecutionResult> createOrGetExecution(ExecutionCreateCommand command) {
        return blocking(() -> createOrGetExecutionBlocking(command));
    }

    @Override
    public Uni<Optional<ExecutionRecord<Object, Object>>> getExecution(String tenantId, String executionId) {
        return blocking(() -> getExecutionBlocking(tenantId, executionId, System.currentTimeMillis()));
    }

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
            return Uni.createFrom().failure(new IllegalArgumentException("Unsupported terminal status: " + finalStatus));
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

    @Override
    public Uni<List<ExecutionRecord<Object, Object>>> findDueExecutions(long nowEpochMs, int limit) {
        if (limit <= 0) {
            return Uni.createFrom().item(List.of());
        }
        return blocking(() -> findDueExecutionsBlocking(nowEpochMs, limit));
    }

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

    private Optional<ExecutionRecord<Object, Object>> claimLeaseBlocking(
        String tenantId,
        String executionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs
    ) {
        if (leaseMs <= 0) {
            throw new IllegalArgumentException("leaseMs must be > 0 for claimLease.");
        }
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

    private static void putIfPresent(Map<String, AttributeValue> item, String key, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        item.put(key, avS(value));
    }

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

    private Object readPayload(AttributeValue value) {
        if (value == null || value.s() == null || value.s().isBlank()) {
            return null;
        }
        return fromJson(value.s());
    }

    private String executionTable() {
        return orchestratorConfig.dynamo().executionTable();
    }

    private String executionKeyTable() {
        return orchestratorConfig.dynamo().executionKeyTable();
    }

    private static Map<String, AttributeValue> executionPrimaryKey(String tenantId, String executionId) {
        return Map.of(
            TENANT_ID, avS(tenantId),
            EXECUTION_ID, avS(executionId));
    }

    private static boolean isExpired(ExecutionRecord<Object, Object> record, long nowEpochMs) {
        if (record.ttlEpochS() <= 0) {
            return false;
        }
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        return record.ttlEpochS() <= nowEpochS;
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String readString(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null) {
            return null;
        }
        return value.s();
    }

    private static long readLong(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null || value.n() == null || value.n().isBlank()) {
            return 0L;
        }
        return Long.parseLong(value.n());
    }

    private static String scopedExecutionKey(String tenantId, String executionKey) {
        String safeTenant = Objects.requireNonNull(tenantId, "tenantId must not be null");
        String safeKey = Objects.requireNonNull(executionKey, "executionKey must not be null");
        return safeTenant.length() + ":" + safeTenant + ":" + safeKey.length() + ":" + safeKey;
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

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return PipelineJson.mapper().writeValueAsString(encodeValue(value));
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing execution payload to JSON.", e);
        }
    }

    private Object fromJson(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return decodeValue(PipelineJson.mapper().readValue(value, Object.class));
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed deserializing execution payload JSON.", e);
        }
    }

    private Object encodeValue(Object value) {
        if (value instanceof Message message) {
            return Map.of(ENCODED_INTERNAL, protobufEnvelope(message));
        }
        if (value instanceof Iterable<?> iterable) {
            return StreamSupport.stream(iterable.spliterator(), false)
                .map(this::encodeValue)
                .toList();
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> encoded = new HashMap<>(map.size());
            map.forEach((key, nestedValue) -> encoded.put(key, encodeValue(nestedValue)));
            return containsReservedEnvelopeKeys(encoded) ? Map.of(ENCODED_ESCAPED_MAP, encoded) : encoded;
        }
        return value;
    }

    private Object decodeValue(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(this::decodeValue).toList();
        }
        if (!(value instanceof Map<?, ?> map)) {
            return value;
        }
        if (isWrappedEnvelope(map)) {
            return decodeProtobufEnvelope(requireEnvelopeMap(map.get(ENCODED_INTERNAL)));
        }
        if (isEscapedUserMap(map)) {
            return decodeValue(requireEnvelopeMap(map.get(ENCODED_ESCAPED_MAP)));
        }
        Map<Object, Object> decoded = new HashMap<>(map.size());
        map.forEach((key, nestedValue) -> decoded.put(key, decodeValue(nestedValue)));
        return decoded;
    }

    private Object decodeProtobufEnvelope(Map<?, ?> map) {
        String messageType = Objects.toString(map.get(ENCODED_MESSAGE_NAME), "");
        String messageJavaClass = Objects.toString(map.get(ENCODED_MESSAGE_JAVA_CLASS), "");
        String payload = Objects.toString(map.get(ENCODED_PAYLOAD), "");
        if (messageType.isBlank() || payload.isBlank()) {
            throw new IllegalStateException("Stored protobuf payload metadata is incomplete.");
        }
        ProtobufMessageParser parser = findProtobufParser(messageType)
            .orElseGet(() -> reflectiveParser(messageType, messageJavaClass));
        try {
            return parser.parseFrom(Base64.getDecoder().decode(payload));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "Stored protobuf payload metadata is corrupted for messageType=" + messageType
                    + ": failed to decode " + ENCODED_PAYLOAD + ".",
                e);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                "Stored protobuf payload metadata is corrupted for messageType=" + messageType
                    + ": protobuf payload bytes are invalid.",
                e);
        }
    }

    private Optional<ProtobufMessageParser> findProtobufParser(String messageType) {
        if (protobufMessageParsers == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(protobufParserLookup().get(normalizeMessageType(messageType)));
    }

    private ProtobufMessageParser reflectiveParser(String messageType, String messageJavaClass) {
        Class<? extends Message> messageClass = loadProtobufMessageClass(messageType, messageJavaClass)
            .orElseThrow(() -> new IllegalStateException("No protobuf parser registered for " + messageType));
        return new ProtobufMessageParser() {
            @Override
            public String type() {
                return messageType;
            }

            @Override
            public Message parseFrom(byte[] bytes) {
                try {
                    Method parseFrom = messageClass.getMethod("parseFrom", byte[].class);
                    Object parsed = parseFrom.invoke(null, bytes);
                    if (parsed instanceof Message message) {
                        return message;
                    }
                    throw new IllegalStateException(
                        "Static parseFrom(byte[]) on " + messageClass.getName() + " did not return a protobuf Message.");
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException(
                        "Failed to parse protobuf payload reflectively for messageType=" + messageType + ".",
                        e);
                }
            }
        };
    }

    private Optional<Class<? extends Message>> loadProtobufMessageClass(String messageType) {
        return loadProtobufMessageClass(messageType, null);
    }

    private Optional<Class<? extends Message>> loadProtobufMessageClass(String messageType, String messageJavaClass) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = DynamoExecutionStateStore.class.getClassLoader();
        }
        if (messageJavaClass != null && !messageJavaClass.isBlank()) {
            Optional<Class<? extends Message>> exactJavaClass = loadProtobufMessageClassCandidate(classLoader, messageJavaClass);
            if (exactJavaClass.isPresent()) {
                return exactJavaClass;
            }
        }
        for (String candidate : protobufMessageTypeCandidates(messageType)) {
            Optional<Class<? extends Message>> resolved = loadProtobufMessageClassCandidate(classLoader, candidate);
            if (resolved.isPresent()) {
                return resolved;
            }
        }
        return Optional.empty();
    }

    private Optional<Class<? extends Message>> loadProtobufMessageClassCandidate(ClassLoader classLoader, String candidate) {
        try {
            Class<?> loaded = classLoader.loadClass(candidate);
            if (Message.class.isAssignableFrom(loaded)) {
                @SuppressWarnings("unchecked")
                Class<? extends Message> messageClass = (Class<? extends Message>) loaded;
                return Optional.of(messageClass);
            }
        } catch (ClassNotFoundException ignored) {
            // Keep trying progressively more specific candidates.
        }
        return Optional.empty();
    }

    private static List<String> protobufMessageTypeCandidates(String messageType) {
        List<String> candidates = new ArrayList<>();
        if (messageType == null || messageType.isBlank()) {
            return candidates;
        }
        String candidate = normalizeMessageType(messageType);
        addProtobufMessageTypeCandidates(candidates, candidate);
        if (candidate.startsWith("google.protobuf.")) {
            addProtobufMessageTypeCandidates(candidates, "com." + candidate);
        }
        return candidates;
    }

    private static void addProtobufMessageTypeCandidates(List<String> candidates, String candidate) {
        candidates.add(candidate);
        int index = candidate.lastIndexOf('.');
        while (index > 0) {
            candidate = candidate.substring(0, index) + '$' + candidate.substring(index + 1);
            candidates.add(candidate);
            index = candidate.lastIndexOf('.');
        }
    }

    private Map<String, ProtobufMessageParser> protobufParserLookup() {
        Map<String, ProtobufMessageParser> active = protobufParserLookup;
        if (active != null) {
            return active;
        }
        if (protobufMessageParsers == null) {
            throw new IllegalStateException("No protobuf parsers available.");
        }
        synchronized (this) {
            if (protobufParserLookup == null) {
                Map<String, ProtobufMessageParser> resolved = new HashMap<>();
                protobufMessageParsers.stream().forEach(parser ->
                    resolved.put(normalizeMessageType(parser.type()), parser));
                protobufParserLookup = Map.copyOf(resolved);
            }
            return protobufParserLookup;
        }
    }

    private static String messageTypeName(Message message) {
        return message.getDescriptorForType().getFullName();
    }

    private static Map<String, Object> protobufEnvelope(Message message) {
        return Map.of(
            ENCODED_TYPE, ENCODED_MESSAGE_CLASS,
            ENCODED_MESSAGE_NAME, messageTypeName(message),
            ENCODED_MESSAGE_JAVA_CLASS, message.getClass().getName(),
            ENCODED_PAYLOAD, Base64.getEncoder().encodeToString(message.toByteArray()));
    }

    // Reserved _tpf_* keys are internal persistence metadata and must be escaped in user payload maps.
    private static boolean containsReservedEnvelopeKeys(Map<?, ?> map) {
        return map.containsKey(ENCODED_INTERNAL)
            || map.containsKey(ENCODED_ESCAPED_MAP)
            || map.containsKey(ENCODED_TYPE)
            || map.containsKey(ENCODED_MESSAGE_NAME)
            || map.containsKey(ENCODED_MESSAGE_JAVA_CLASS)
            || map.containsKey(ENCODED_PAYLOAD);
    }

    private static boolean isWrappedEnvelope(Map<?, ?> map) {
        return map.size() == 1 && map.containsKey(ENCODED_INTERNAL) && map.get(ENCODED_INTERNAL) instanceof Map<?, ?> nested
            && isProtobufEnvelope(nested);
    }

    private static boolean isEscapedUserMap(Map<?, ?> map) {
        return map.size() == 1 && map.containsKey(ENCODED_ESCAPED_MAP) && map.get(ENCODED_ESCAPED_MAP) instanceof Map<?, ?>;
    }

    private static boolean isProtobufEnvelope(Map<?, ?> map) {
        return (map.size() == 3 || map.size() == 4)
            && ENCODED_MESSAGE_CLASS.equals(map.get(ENCODED_TYPE))
            && map.containsKey(ENCODED_MESSAGE_NAME)
            && (!map.containsKey(ENCODED_MESSAGE_JAVA_CLASS)
                || map.get(ENCODED_MESSAGE_JAVA_CLASS) instanceof String)
            && map.containsKey(ENCODED_PAYLOAD);
    }

    private static Map<?, ?> requireEnvelopeMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return map;
        }
        throw new IllegalStateException("Stored payload metadata wrapper is malformed.");
    }

    private static String normalizeMessageType(String messageType) {
        return messageType == null ? "" : messageType.replace('$', '.');
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = DynamoDbClient.builder();
                builder.httpClientBuilder(UrlConnectionHttpClient.builder());
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

    private static <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }
}
