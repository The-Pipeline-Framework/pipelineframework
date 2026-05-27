package org.pipelineframework.awaitable.store;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * DynamoDB-backed await unit store.
 */
@ApplicationScoped
public class DynamoAwaitUnitStore implements AwaitUnitStore {
    private static final Logger LOG = Logger.getLogger(DynamoAwaitUnitStore.class);

    private static final String TENANT_ID = "tenant_id";
    private static final String UNIT_ID = "unit_id";
    private static final String EXECUTION_ID = "execution_id";
    private static final String STEP_ID = "step_id";
    private static final String STEP_INDEX = "step_index";
    private static final String CARDINALITY = "cardinality";
    private static final String VERSION = "version";
    private static final String STATUS = "status";
    private static final String PRIMARY_INTERACTION_ID = "primary_interaction_id";
    private static final String EXPECTED_ITEM_COUNT = "expected_item_count";
    private static final String COMPLETED_ITEM_COUNT = "completed_item_count";
    private static final String DISPATCH_COMPLETE = "dispatch_complete";
    private static final String CREATED_AT_EPOCH_MS = "created_at_epoch_ms";
    private static final String UPDATED_AT_EPOCH_MS = "updated_at_epoch_ms";
    private static final String TTL_EPOCH_S = "ttl_epoch_s";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private volatile DynamoDbClient client;

    @Override
    public String providerName() {
        return "dynamo";
    }

    @Override
    public int priority() {
        return -1000;
    }

    @Override
    public Uni<AwaitUnitRecord> createOrGet(AwaitUnitCreateCommand command) {
        return blocking(() -> {
            Optional<AwaitUnitRecord> existing = getBlocking(command.tenantId(), command.unitId(), command.nowEpochMs());
            if (existing.isPresent()) {
                return existing.get();
            }
            AwaitUnitRecord created = new AwaitUnitRecord(
                command.tenantId(),
                command.unitId(),
                command.executionId(),
                command.stepId(),
                command.stepIndex(),
                command.cardinality(),
                0L,
                AwaitUnitStatus.WAITING_EXTERNAL,
                null,
                null,
                0,
                false,
                command.nowEpochMs(),
                command.nowEpochMs(),
                command.ttlEpochS());
            try {
                long nowEpochS = System.currentTimeMillis() / 1000;
                dynamoClient().putItem(PutItemRequest.builder()
                    .tableName(unitTable())
                    .item(toItem(created))
                    .conditionExpression("(attribute_not_exists(#tenant) AND attribute_not_exists(#unit)) OR #ttl < :now")
                    .expressionAttributeNames(Map.of("#tenant", TENANT_ID, "#unit", UNIT_ID, "#ttl", TTL_EPOCH_S))
                    .expressionAttributeValues(Map.of(":now", AttributeValue.builder().n(String.valueOf(nowEpochS)).build()))
                    .build());
                return created;
            } catch (ConditionalCheckFailedException ignored) {
                return getBlocking(command.tenantId(), command.unitId(), command.nowEpochMs())
                    .orElseThrow(() -> new IllegalStateException("Await unit create lost race but no unit was found"));
            }
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> get(String tenantId, String unitId) {
        return blocking(() -> getBlocking(tenantId, unitId, System.currentTimeMillis()));
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> attachPrimaryInteraction(
        String tenantId,
        String unitId,
        String interactionId,
        long nowEpochMs) {
        return update(
            tenantId,
            unitId,
            "SET #primary = :primary, #updated = :updated, #version = #version + :one",
            Map.of("#primary", PRIMARY_INTERACTION_ID, "#updated", UPDATED_AT_EPOCH_MS, "#version", VERSION),
            Map.of(":primary", avS(interactionId), ":updated", avN(nowEpochMs), ":one", avN(1)));
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markDispatchComplete(
        String tenantId,
        String unitId,
        int expectedItemCount,
        long nowEpochMs) {
        return blocking(() -> {
            Optional<AwaitUnitRecord> current = getBlocking(tenantId, unitId, nowEpochMs);
            if (current.isEmpty()) {
                return Optional.empty();
            }
            AwaitUnitStatus status = current.get().completedItemCount() == expectedItemCount
                ? AwaitUnitStatus.COMPLETED
                : current.get().status();
            Map<String, String> names = Map.of(
                "#expected", EXPECTED_ITEM_COUNT,
                "#dispatch", DISPATCH_COMPLETE,
                "#status", STATUS,
                "#updated", UPDATED_AT_EPOCH_MS,
                "#version", VERSION);
            Map<String, AttributeValue> values = Map.of(
                ":expected", avN(expectedItemCount),
                ":dispatch", AttributeValue.builder().bool(true).build(),
                ":status", avS(status.name()),
                ":updated", avN(nowEpochMs),
                ":one", avN(1));
            return updateBlocking(tenantId, unitId,
                "SET #expected = :expected, #dispatch = :dispatch, #status = :status, #updated = :updated, #version = #version + :one",
                names,
                values);
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> recordItemCompleted(String tenantId, String unitId, long nowEpochMs) {
        return blocking(() -> {
            Map<String, String> names = Map.of(
                "#completed", COMPLETED_ITEM_COUNT,
                "#updated", UPDATED_AT_EPOCH_MS,
                "#version", VERSION);
            Map<String, AttributeValue> values = Map.of(
                ":updated", avN(nowEpochMs),
                ":one", avN(1));
            Optional<AwaitUnitRecord> incremented = updateBlocking(tenantId, unitId,
                "SET #updated = :updated ADD #completed :one, #version :one",
                names,
                values,
                existingNonTerminalCondition());
            if (incremented.isEmpty()) {
                return getBlocking(tenantId, unitId, nowEpochMs);
            }
            AwaitUnitRecord updated = incremented.get();
            if (!updated.dispatchComplete()
                || updated.expectedItemCount() == null
                || updated.completedItemCount() < updated.expectedItemCount()) {
                return incremented;
            }
            Optional<AwaitUnitRecord> completed = updateBlocking(tenantId, unitId,
                "SET #status = :status, #updated = :updated ADD #version :one",
                Map.of("#status", STATUS, "#completed", COMPLETED_ITEM_COUNT, "#expected", EXPECTED_ITEM_COUNT,
                    "#updated", UPDATED_AT_EPOCH_MS, "#version", VERSION),
                Map.of(":status", avS(AwaitUnitStatus.COMPLETED.name()), ":updated", avN(nowEpochMs),
                    ":one", avN(1)),
                existingNonTerminalCondition() + " AND #completed >= #expected");
            return completed.isPresent() ? completed : getBlocking(tenantId, unitId, nowEpochMs);
        });
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markCompleted(String tenantId, String unitId, long nowEpochMs) {
        return update(
            tenantId,
            unitId,
            "SET #status = :status, #updated = :updated, #version = #version + :one",
            Map.of("#status", STATUS, "#updated", UPDATED_AT_EPOCH_MS, "#version", VERSION),
            Map.of(":status", avS(AwaitUnitStatus.COMPLETED.name()), ":updated", avN(nowEpochMs), ":one", avN(1)));
    }

    @Override
    public Uni<Optional<AwaitUnitRecord>> markTerminal(
        String tenantId,
        String unitId,
        AwaitUnitStatus status,
        long nowEpochMs) {
        return update(
            tenantId,
            unitId,
            "SET #status = :status, #updated = :updated, #version = #version + :one",
            Map.of("#status", STATUS, "#updated", UPDATED_AT_EPOCH_MS, "#version", VERSION),
            Map.of(":status", avS(status.name()), ":updated", avN(nowEpochMs), ":one", avN(1)));
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

    private Uni<Optional<AwaitUnitRecord>> update(
        String tenantId,
        String unitId,
        String updateExpression,
        Map<String, String> names,
        Map<String, AttributeValue> values) {
        return blocking(() -> updateBlocking(tenantId, unitId, updateExpression, names, values));
    }

    private Optional<AwaitUnitRecord> updateBlocking(
        String tenantId,
        String unitId,
        String updateExpression,
        Map<String, String> names,
        Map<String, AttributeValue> values) {
        return updateBlocking(tenantId, unitId, updateExpression, names, values,
            "attribute_exists(#tenant) AND attribute_exists(#unit)");
    }

    private Optional<AwaitUnitRecord> updateBlocking(
        String tenantId,
        String unitId,
        String updateExpression,
        Map<String, String> names,
        Map<String, AttributeValue> values,
        String conditionExpression) {
        try {
            var response = dynamoClient().updateItem(UpdateItemRequest.builder()
                .tableName(unitTable())
                .key(Map.of(TENANT_ID, avS(tenantId), UNIT_ID, avS(unitId)))
                .updateExpression(updateExpression)
                .conditionExpression(conditionExpression)
                .expressionAttributeNames(mergeNames(names, conditionExpression))
                .expressionAttributeValues(mergeTerminalValues(values, conditionExpression))
                .returnValues(ReturnValue.ALL_NEW)
                .build());
            return Optional.of(toRecord(response.attributes()));
        } catch (ConditionalCheckFailedException ignored) {
            return Optional.empty();
        }
    }

    private Optional<AwaitUnitRecord> getBlocking(String tenantId, String unitId, long nowEpochMs) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(unitTable())
            .key(Map.of(TENANT_ID, avS(tenantId), UNIT_ID, avS(unitId)))
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        AwaitUnitRecord record = toRecord(item);
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        if (record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS) {
            return Optional.empty();
        }
        return Optional.of(record);
    }

    private Map<String, AttributeValue> toItem(AwaitUnitRecord record) {
        java.util.LinkedHashMap<String, AttributeValue> item = new java.util.LinkedHashMap<>();
        item.put(TENANT_ID, avS(record.tenantId()));
        item.put(UNIT_ID, avS(record.unitId()));
        item.put(EXECUTION_ID, avS(record.executionId()));
        item.put(STEP_ID, avS(record.stepId()));
        item.put(STEP_INDEX, avN(record.stepIndex()));
        item.put(CARDINALITY, avS(record.cardinality()));
        item.put(VERSION, avN(record.version()));
        item.put(STATUS, avS(record.status().name()));
        if (record.primaryInteractionId() != null) {
            item.put(PRIMARY_INTERACTION_ID, avS(record.primaryInteractionId()));
        }
        if (record.expectedItemCount() != null) {
            item.put(EXPECTED_ITEM_COUNT, avN(record.expectedItemCount()));
        }
        item.put(COMPLETED_ITEM_COUNT, avN(record.completedItemCount()));
        item.put(DISPATCH_COMPLETE, AttributeValue.builder().bool(record.dispatchComplete()).build());
        item.put(CREATED_AT_EPOCH_MS, avN(record.createdAtEpochMs()));
        item.put(UPDATED_AT_EPOCH_MS, avN(record.updatedAtEpochMs()));
        item.put(TTL_EPOCH_S, avN(record.ttlEpochS()));
        return item;
    }

    private AwaitUnitRecord toRecord(Map<String, AttributeValue> item) {
        return new AwaitUnitRecord(
            readString(item, TENANT_ID),
            readString(item, UNIT_ID),
            readString(item, EXECUTION_ID),
            readString(item, STEP_ID),
            readInt(item, STEP_INDEX),
            readString(item, CARDINALITY),
            readLong(item, VERSION),
            AwaitUnitStatus.valueOf(readString(item, STATUS)),
            readString(item, PRIMARY_INTERACTION_ID),
            readNullableInt(item, EXPECTED_ITEM_COUNT),
            readInt(item, COMPLETED_ITEM_COUNT),
            item.containsKey(DISPATCH_COMPLETE) && Boolean.TRUE.equals(item.get(DISPATCH_COMPLETE).bool()),
            readLong(item, CREATED_AT_EPOCH_MS),
            readLong(item, UPDATED_AT_EPOCH_MS),
            readLong(item, TTL_EPOCH_S));
    }

    private Map<String, String> mergeNames(Map<String, String> names, String conditionExpression) {
        java.util.LinkedHashMap<String, String> merged = new java.util.LinkedHashMap<>(names);
        merged.put("#tenant", TENANT_ID);
        merged.put("#unit", UNIT_ID);
        if (conditionExpression.contains("#status")) {
            merged.putIfAbsent("#status", STATUS);
        }
        return merged;
    }

    private Map<String, AttributeValue> mergeTerminalValues(
        Map<String, AttributeValue> values,
        String conditionExpression) {
        java.util.LinkedHashMap<String, AttributeValue> merged = new java.util.LinkedHashMap<>(values);
        if (conditionExpression.contains(":completedStatus")) {
            merged.putIfAbsent(":completedStatus", avS(AwaitUnitStatus.COMPLETED.name()));
            merged.putIfAbsent(":failedStatus", avS(AwaitUnitStatus.FAILED.name()));
            merged.putIfAbsent(":timedOutStatus", avS(AwaitUnitStatus.TIMED_OUT.name()));
            merged.putIfAbsent(":cancelledStatus", avS(AwaitUnitStatus.CANCELLED.name()));
            merged.putIfAbsent(":expiredStatus", avS(AwaitUnitStatus.EXPIRED.name()));
        }
        return merged;
    }

    private String existingNonTerminalCondition() {
        return "attribute_exists(#tenant) AND attribute_exists(#unit)"
            + " AND #status <> :completedStatus"
            + " AND #status <> :failedStatus"
            + " AND #status <> :timedOutStatus"
            + " AND #status <> :cancelledStatus"
            + " AND #status <> :expiredStatus";
    }

    private String unitTable() {
        return orchestratorConfig.dynamo().awaitUnitTable();
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient cached = client;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            if (client == null) {
                var builder = DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
                orchestratorConfig.dynamo().region().filter(value -> !value.isBlank()).map(Region::of).ifPresent(builder::region);
                orchestratorConfig.dynamo().endpointOverride().filter(value -> !value.isBlank()).map(URI::create).ifPresent(builder::endpointOverride);
                client = builder.build();
            }
            return client;
        }
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String readString(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? null : value.s();
    }

    private static long readLong(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? 0L : Long.parseLong(value.n());
    }

    private static int readInt(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? 0 : Integer.parseInt(value.n());
    }

    private static Integer readNullableInt(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? null : Integer.parseInt(value.n());
    }

    private static <T> Uni<T> blocking(java.util.concurrent.Callable<T> callable) {
        return Uni.createFrom().item(() -> {
            try {
                return callable.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Dynamo await unit store operation failed", e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }
}
