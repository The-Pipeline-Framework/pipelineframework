package org.pipelineframework.awaitable.store;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * DynamoDB-backed await interaction store.
 */
@ApplicationScoped
public class DynamoAwaitInteractionStore implements AwaitInteractionStore {
    private static final Logger LOG = Logger.getLogger(DynamoAwaitInteractionStore.class);

    private static final String TENANT_ID = "tenant_id";
    private static final String INTERACTION_ID = "interaction_id";
    private static final String LOOKUP_KEY = "lookup_key";
    private static final String LOOKUP_KIND = "lookup_kind";
    private static final String EXECUTION_ID = "execution_id";
    private static final String STEP_ID = "step_id";
    private static final String STEP_INDEX = "step_index";
    private static final String OUTPUT_TYPE = "output_type";
    private static final String CORRELATION_ID = "correlation_id";
    private static final String CAUSATION_ID = "causation_id";
    private static final String IDEMPOTENCY_KEY = "idempotency_key";
    private static final String VERSION = "version";
    private static final String STATUS = "status";
    private static final String REQUEST_PAYLOAD_JSON = "request_payload_json";
    private static final String RESPONSE_PAYLOAD_JSON = "response_payload_json";
    private static final String ACTOR = "actor";
    private static final String ASSIGNEE = "assignee";
    private static final String GROUP = "group_name";
    private static final String TRANSPORT_TYPE = "transport_type";
    private static final String TRANSPORT_METADATA_JSON = "transport_metadata_json";
    private static final String DEADLINE_EPOCH_MS = "deadline_epoch_ms";
    private static final String CREATED_AT_EPOCH_MS = "created_at_epoch_ms";
    private static final String UPDATED_AT_EPOCH_MS = "updated_at_epoch_ms";
    private static final String TTL_EPOCH_S = "ttl_epoch_s";
    private static final String ENCODED_JAVA_CLASS = "_tpf_java_class";
    private static final String ENCODED_PAYLOAD = "_tpf_payload";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private volatile DynamoDbClient client;

    /**
     * Default constructor for CDI.
     */
    public DynamoAwaitInteractionStore() {
    }

    DynamoAwaitInteractionStore(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
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
    public Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand command) {
        return blocking(() -> createOrGetBlocking(command));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId) {
        return blocking(() -> getBlocking(tenantId, interactionId, System.currentTimeMillis()));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId) {
        return blocking(() -> findByLookup(
            lookupKey("correlation", tenantId, correlationId),
            tenantId,
            System.currentTimeMillis()));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatched(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.DISPATCHED,
            null,
            null,
            transportMetadata,
            nowEpochMs));
    }

    @Override
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        return blocking(() -> completeBlocking(command));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> fail(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.FAILED,
            null,
            null,
            null,
            nowEpochMs));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> cancel(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.CANCELLED,
            null,
            null,
            null,
            nowEpochMs));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return blocking(() -> transitionStatus(
            tenantId,
            interactionId,
            expectedVersion,
            AwaitInteractionStatus.TIMED_OUT,
            null,
            null,
            null,
            nowEpochMs));
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return blocking(() -> {
            if (limit <= 0) {
                return List.of();
            }
            Map<String, String> names = Map.of(
                "#status", STATUS,
                "#deadline", DEADLINE_EPOCH_MS,
                "#ttl", TTL_EPOCH_S);
            Map<String, AttributeValue> values = Map.of(
                ":now", avN(nowEpochMs),
                ":completed", avS(AwaitInteractionStatus.COMPLETED.name()),
                ":failed", avS(AwaitInteractionStatus.FAILED.name()),
                ":timedOut", avS(AwaitInteractionStatus.TIMED_OUT.name()),
                ":cancelled", avS(AwaitInteractionStatus.CANCELLED.name()),
                ":expired", avS(AwaitInteractionStatus.EXPIRED.name()),
                ":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));
            List<AwaitInteractionRecord> records = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                ScanRequest.Builder request = ScanRequest.builder()
                    .tableName(interactionTable())
                    .filterExpression("#deadline <= :now AND #status <> :completed AND #status <> :failed "
                        + "AND #status <> :timedOut AND #status <> :cancelled AND #status <> :expired "
                        + "AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .limit(scanPageLimit(limit));
                if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                    request.exclusiveStartKey(lastEvaluatedKey);
                }
                var response = dynamoClient().scan(request.build());
                if (response.items() != null) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        records.add(toRecord(item));
                        if (records.size() >= limit) {
                            break;
                        }
                    }
                }
                lastEvaluatedKey = response.lastEvaluatedKey();
            } while (records.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            records.sort(java.util.Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs));
            return List.copyOf(records.subList(0, Math.min(records.size(), limit)));
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return blocking(() -> {
            if (limit <= 0) {
                return List.of();
            }
            Map<String, String> names = new HashMap<>(Map.of("#tenant", TENANT_ID, "#status", STATUS, "#ttl", TTL_EPOCH_S));
            Map<String, AttributeValue> values = new HashMap<>(Map.of(
                ":tenant", avS(tenantId),
                ":completed", avS(AwaitInteractionStatus.COMPLETED.name()),
                ":failed", avS(AwaitInteractionStatus.FAILED.name()),
                ":timedOut", avS(AwaitInteractionStatus.TIMED_OUT.name()),
                ":cancelled", avS(AwaitInteractionStatus.CANCELLED.name()),
                ":expired", avS(AwaitInteractionStatus.EXPIRED.name()),
                ":nowSec", avN(Instant.now().getEpochSecond())));
            StringBuilder filter = new StringBuilder("#tenant = :tenant AND #status <> :completed AND #status <> :failed "
                + "AND #status <> :timedOut AND #status <> :cancelled AND #status <> :expired "
                + "AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)");
            if (assignee != null) {
                names.put("#assignee", ASSIGNEE);
                values.put(":assignee", avS(assignee));
                filter.append(" AND #assignee = :assignee");
            }
            if (group != null) {
                names.put("#group", GROUP);
                values.put(":group", avS(group));
                filter.append(" AND #group = :group");
            }
            if (stepId != null) {
                names.put("#stepId", STEP_ID);
                values.put(":stepId", avS(stepId));
                filter.append(" AND #stepId = :stepId");
            }
            List<AwaitInteractionRecord> records = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                ScanRequest.Builder request = ScanRequest.builder()
                    .tableName(interactionTable())
                    .filterExpression(filter.toString())
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .limit(scanPageLimit(limit));
                if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                    request.exclusiveStartKey(lastEvaluatedKey);
                }
                var response = dynamoClient().scan(request.build());
                if (response.items() != null) {
                    for (Map<String, AttributeValue> item : response.items()) {
                        AwaitInteractionRecord record = toRecord(item);
                        records.add(record);
                        if (records.size() >= limit) {
                            break;
                        }
                    }
                }
                lastEvaluatedKey = response.lastEvaluatedKey();
            } while (records.size() < limit && lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
            records.sort(java.util.Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs));
            return List.copyOf(records.subList(0, Math.min(records.size(), limit)));
        });
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

    private AwaitCreateResult createOrGetBlocking(AwaitCreateCommand command) {
        Optional<AwaitInteractionRecord> existing = findByLookup(
            lookupKey("idempotency", command.tenantId(), command.stepId() + ":" + command.idempotencyKey()),
            command.tenantId(),
            command.nowEpochMs());
        if (existing.isPresent()) {
            return new AwaitCreateResult(existing.get(), true);
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
            null,
            command.assignee(),
            command.group(),
            command.transportType(),
            Map.of(),
            command.deadlineEpochMs(),
            command.nowEpochMs(),
            command.nowEpochMs(),
            command.ttlEpochS());

        try {
            dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder()
                .transactItems(
                    TransactWriteItem.builder().put(Put.builder()
                        .tableName(interactionTable())
                        .item(toItem(created))
                        .conditionExpression("attribute_not_exists(#tenant) AND attribute_not_exists(#interaction)")
                        .expressionAttributeNames(Map.of("#tenant", TENANT_ID, "#interaction", INTERACTION_ID))
                        .build()).build(),
                    TransactWriteItem.builder().put(lookupPut(
                        "idempotency",
                        command.tenantId(),
                        command.stepId() + ":" + command.idempotencyKey(),
                        interactionId,
                        command.ttlEpochS())).build(),
                    TransactWriteItem.builder().put(lookupPut(
                        "correlation",
                        command.tenantId(),
                        command.correlationId(),
                        interactionId,
                        command.ttlEpochS())).build())
                .build());
            return new AwaitCreateResult(created, false);
        } catch (TransactionCanceledException | ConditionalCheckFailedException ignored) {
            return findByLookup(
                lookupKey("idempotency", command.tenantId(), command.stepId() + ":" + command.idempotencyKey()),
                command.tenantId(),
                command.nowEpochMs())
                .map(record -> new AwaitCreateResult(record, true))
                .orElseThrow(() -> ignored);
        }
    }

    private AwaitCompletionResult completeBlocking(AwaitCompletionCommand command) {
        AwaitInteractionRecord current = resolveForCompletion(command)
            .orElseThrow(() -> new IllegalArgumentException("No await interaction matches completion"));
        if (current.status() == AwaitInteractionStatus.COMPLETED) {
            return new AwaitCompletionResult(current, true);
        }
        if (current.status().terminal()) {
            throw new IllegalStateException("Await interaction is terminal: " + current.status());
        }
        if (current.deadlineEpochMs() <= command.nowEpochMs()) {
            Optional<AwaitInteractionRecord> timedOut = transitionStatus(
                current.tenantId(),
                current.interactionId(),
                current.version(),
                AwaitInteractionStatus.TIMED_OUT,
                null,
                null,
                null,
                command.nowEpochMs());
            if (timedOut.isEmpty()) {
                Optional<AwaitInteractionRecord> refreshed = getBlocking(
                    current.tenantId(),
                    current.interactionId(),
                    command.nowEpochMs());
                if (refreshed.isPresent() && refreshed.get().status() == AwaitInteractionStatus.COMPLETED) {
                    return new AwaitCompletionResult(refreshed.get(), true);
                }
            }
            throw new IllegalStateException("Await interaction timed out before completion");
        }
        AwaitInteractionRecord completed = transitionStatus(
            current.tenantId(),
            current.interactionId(),
            current.version(),
            AwaitInteractionStatus.COMPLETED,
            command.responsePayload(),
            command.actor(),
            null,
            command.nowEpochMs())
            .orElseThrow(() -> new IllegalStateException("Await completion transition lost OCC race"));
        return new AwaitCompletionResult(completed, false);
    }

    private Optional<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        if (command.interactionId() != null && !command.interactionId().isBlank()) {
            return getBlocking(command.tenantId(), command.interactionId(), command.nowEpochMs());
        }
        if (command.correlationId() != null && !command.correlationId().isBlank()) {
            return findByLookup(
                lookupKey("correlation", command.tenantId(), command.correlationId()),
                command.tenantId(),
                command.nowEpochMs());
        }
        return Optional.empty();
    }

    private Optional<AwaitInteractionRecord> transitionStatus(
        String tenantId,
        String interactionId,
        long expectedVersion,
        AwaitInteractionStatus status,
        Object responsePayload,
        String actor,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        Map<String, String> names = new HashMap<>();
        names.put("#version", VERSION);
        names.put("#status", STATUS);
        names.put("#response", RESPONSE_PAYLOAD_JSON);
        names.put("#actor", ACTOR);
        names.put("#metadata", TRANSPORT_METADATA_JSON);
        names.put("#updated", UPDATED_AT_EPOCH_MS);
        names.put("#ttl", TTL_EPOCH_S);
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":expected", avN(expectedVersion));
        values.put(":status", avS(status.name()));
        values.put(":one", avN(1));
        values.put(":now", avN(nowEpochMs));
        values.put(":nowSec", avN(Instant.ofEpochMilli(nowEpochMs).getEpochSecond()));
        if (responsePayload != null) {
            values.put(":response", avS(toJson(responsePayload)));
        }
        if (actor != null && !actor.isBlank()) {
            values.put(":actor", avS(actor));
        }
        if (transportMetadata != null) {
            values.put(":metadata", avS(toJson(transportMetadata)));
        }
        StringBuilder update = new StringBuilder("SET #status = :status, #version = #version + :one, #updated = :now");
        if (responsePayload != null) {
            update.append(", #response = :response");
        }
        if (actor != null && !actor.isBlank()) {
            update.append(", #actor = :actor");
        }
        if (transportMetadata != null) {
            update.append(", #metadata = :metadata");
        }
        try {
            Map<String, AttributeValue> attributes = dynamoClient().updateItem(UpdateItemRequest.builder()
                .tableName(interactionTable())
                .key(primaryKey(tenantId, interactionId))
                .conditionExpression("#version = :expected AND (attribute_not_exists(#ttl) OR #ttl > :nowSec)")
                .updateExpression(update.toString())
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .returnValues(ReturnValue.ALL_NEW)
                .build()).attributes();
            return attributes == null || attributes.isEmpty() ? Optional.empty() : Optional.of(toRecord(attributes));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        }
    }

    private Optional<AwaitInteractionRecord> getBlocking(String tenantId, String interactionId, long nowEpochMs) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(interactionTable())
            .key(primaryKey(tenantId, interactionId))
            .consistentRead(true)
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        AwaitInteractionRecord record = toRecord(item);
        return record.ttlEpochS() > 0 && record.ttlEpochS() <= Instant.ofEpochMilli(nowEpochMs).getEpochSecond()
            ? Optional.empty()
            : Optional.of(record);
    }

    private Optional<AwaitInteractionRecord> findByLookup(String lookupKey, String tenantId, long nowEpochMs) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(lookupTable())
            .key(Map.of(LOOKUP_KEY, avS(lookupKey)))
            .consistentRead(true)
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        String interactionId = readString(item, INTERACTION_ID);
        return interactionId == null || interactionId.isBlank()
            ? Optional.empty()
            : getBlocking(tenantId, interactionId, nowEpochMs);
    }

    private Put lookupPut(String kind, String tenantId, String key, String interactionId, long ttlEpochS) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(LOOKUP_KEY, avS(lookupKey(kind, tenantId, key)));
        item.put(LOOKUP_KIND, avS(kind));
        item.put(TENANT_ID, avS(tenantId));
        item.put(INTERACTION_ID, avS(interactionId));
        item.put(TTL_EPOCH_S, avN(ttlEpochS));
        return Put.builder()
            .tableName(lookupTable())
            .item(item)
            .conditionExpression("attribute_not_exists(#lookup)")
            .expressionAttributeNames(Map.of("#lookup", LOOKUP_KEY))
            .build();
    }

    private Map<String, AttributeValue> toItem(AwaitInteractionRecord record) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TENANT_ID, avS(record.tenantId()));
        item.put(INTERACTION_ID, avS(record.interactionId()));
        item.put(EXECUTION_ID, avS(record.executionId()));
        item.put(STEP_ID, avS(record.stepId()));
        item.put(STEP_INDEX, avN(record.stepIndex()));
        item.put(OUTPUT_TYPE, avS(record.outputType()));
        item.put(CORRELATION_ID, avS(record.correlationId()));
        item.put(IDEMPOTENCY_KEY, avS(record.idempotencyKey()));
        item.put(VERSION, avN(record.version()));
        item.put(STATUS, avS(record.status().name()));
        putIfPresent(item, REQUEST_PAYLOAD_JSON, toJson(record.requestPayload()));
        item.put(TRANSPORT_TYPE, avS(record.transportType()));
        putIfPresent(item, TRANSPORT_METADATA_JSON, toJson(record.transportMetadata()));
        item.put(DEADLINE_EPOCH_MS, avN(record.deadlineEpochMs()));
        item.put(CREATED_AT_EPOCH_MS, avN(record.createdAtEpochMs()));
        item.put(UPDATED_AT_EPOCH_MS, avN(record.updatedAtEpochMs()));
        item.put(TTL_EPOCH_S, avN(record.ttlEpochS()));
        putIfPresent(item, CAUSATION_ID, record.causationId());
        putIfPresent(item, RESPONSE_PAYLOAD_JSON, toJson(record.responsePayload()));
        putIfPresent(item, ACTOR, record.actor());
        putIfPresent(item, ASSIGNEE, record.assignee());
        putIfPresent(item, GROUP, record.group());
        return item;
    }

    @SuppressWarnings("unchecked")
    private AwaitInteractionRecord toRecord(Map<String, AttributeValue> item) {
        return new AwaitInteractionRecord(
            readString(item, TENANT_ID),
            readString(item, EXECUTION_ID),
            readString(item, STEP_ID),
            Math.toIntExact(readLong(item, STEP_INDEX)),
            readString(item, OUTPUT_TYPE),
            readString(item, INTERACTION_ID),
            readString(item, CORRELATION_ID),
            readString(item, CAUSATION_ID),
            readString(item, IDEMPOTENCY_KEY),
            readLong(item, VERSION),
            AwaitInteractionStatus.valueOf(readString(item, STATUS)),
            fromJson(readString(item, REQUEST_PAYLOAD_JSON)),
            fromJson(readString(item, RESPONSE_PAYLOAD_JSON)),
            readString(item, ACTOR),
            readString(item, ASSIGNEE),
            readString(item, GROUP),
            readString(item, TRANSPORT_TYPE),
            (Map<String, Object>) Optional.ofNullable(fromJson(readString(item, TRANSPORT_METADATA_JSON))).orElse(Map.of()),
            readLong(item, DEADLINE_EPOCH_MS),
            readLong(item, CREATED_AT_EPOCH_MS),
            readLong(item, UPDATED_AT_EPOCH_MS),
            readLong(item, TTL_EPOCH_S));
    }

    private <T> Uni<T> blocking(Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultExecutor());
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            active = client;
            if (active != null) {
                return active;
            }
            var builder = DynamoDbClient.builder();
            builder.httpClientBuilder(UrlConnectionHttpClient.builder()
                .connectionTimeout(java.time.Duration.ofSeconds(10))
                .socketTimeout(java.time.Duration.ofSeconds(30)));
            orchestratorConfig.dynamo().region().filter(region -> !region.isBlank())
                .ifPresent(region -> builder.region(Region.of(region)));
            orchestratorConfig.dynamo().endpointOverride()
                .filter(endpoint -> !endpoint.isBlank())
                .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
            client = builder.build();
            return client;
        }
    }

    private String interactionTable() {
        return orchestratorConfig.dynamo().awaitInteractionTable();
    }

    private String lookupTable() {
        return orchestratorConfig.dynamo().awaitInteractionKeyTable();
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return PipelineJson.mapper().writeValueAsString(encode(value));
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing await payload to JSON.", e);
        }
    }

    private Object fromJson(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            Object decoded = PipelineJson.mapper().readValue(value, Object.class);
            if (decoded instanceof Map<?, ?> map && map.containsKey(ENCODED_JAVA_CLASS)) {
                String className = String.valueOf(map.get(ENCODED_JAVA_CLASS));
                Object payload = map.get(ENCODED_PAYLOAD);
                return PipelineJson.mapper().convertValue(payload, Class.forName(className));
            }
            return decoded;
        } catch (Exception e) {
            throw new IllegalStateException("Failed deserializing await payload JSON.", e);
        }
    }

    private Object encode(Object value) {
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean
            || value instanceof Map<?, ?> || value instanceof Iterable<?>) {
            return value;
        }
        return Map.of(
            ENCODED_JAVA_CLASS, value.getClass().getName(),
            ENCODED_PAYLOAD, PipelineJson.mapper().convertValue(value, Object.class));
    }

    private static String lookupKey(String kind, String tenantId, String key) {
        return kind + ":" + tenantId.length() + ":" + tenantId + ":" + key.length() + ":" + key;
    }

    private static Map<String, AttributeValue> primaryKey(String tenantId, String interactionId) {
        return Map.of(TENANT_ID, avS(tenantId), INTERACTION_ID, avS(interactionId));
    }

    private static AttributeValue avS(String value) {
        return value == null ? null : AttributeValue.builder().s(value).build();
    }

    private static int scanPageLimit(int limit) {
        return (int) Math.min(Math.max((long) limit * 3L, (long) limit), 1_000L);
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String readString(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        return value == null ? null : value.s();
    }

    private static long readLong(Map<String, AttributeValue> item, String key) {
        AttributeValue value = item.get(key);
        if (value == null || value.n() == null || value.n().isBlank()) {
            return 0L;
        }
        return Long.parseLong(value.n());
    }

    private static void putIfPresent(Map<String, AttributeValue> item, String key, String value) {
        if (value != null && !value.isBlank()) {
            item.put(key, avS(value));
        }
    }
}
