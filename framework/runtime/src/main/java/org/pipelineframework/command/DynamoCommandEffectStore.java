package org.pipelineframework.command;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * DynamoDB-backed Command effect authority using immutable, append-only state revisions.
 *
 * <p>The table must use the string partition key {@value #COMMAND_KEY} and numeric sort key
 * {@value #REVISION}. Each transition reads the current revision consistently and conditionally
 * creates the next revision. Concurrent writers therefore compete for the same immutable key;
 * no mutable current-state pointer or {@code UpdateItem} operation is used.</p>
 */
@ApplicationScoped
@IfBuildProperty(name = "pipeline.command.effect-store.provider", stringValue = "dynamo")
public class DynamoCommandEffectStore implements CommandEffectStore {
    public static final String COMMAND_KEY = "command_key";
    public static final String REVISION = "revision";
    public static final String RECORD_JSON = "record_json";
    public static final String SCHEMA_VERSION = "schema_version";
    public static final int MAX_RECORD_BYTES = 300 * 1024;

    private static final Base64.Encoder KEY_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final DynamoDbClient client;
    private final String tableName;
    private final CommandEffectRecordCodec codec;

    @Inject
    public DynamoCommandEffectStore(DynamoDbClient client, CommandEffectStoreConfig config) {
        this(client, Objects.requireNonNull(config, "command effect store config must not be null").dynamo().table());
    }

    /** Creates a store for an explicitly managed client and table. The client remains caller-owned. */
    public DynamoCommandEffectStore(DynamoDbClient client, String tableName) {
        this(client, tableName, new CommandEffectRecordCodec());
    }

    DynamoCommandEffectStore(DynamoDbClient client, String tableName, CommandEffectRecordCodec codec) {
        this.client = Objects.requireNonNull(client, "DynamoDB client must not be null");
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Command effect DynamoDB table must not be blank");
        }
        this.tableName = tableName;
        this.codec = Objects.requireNonNull(codec, "command effect codec must not be null");
    }

    @Override
    public Uni<Optional<CommandEffectRecord>> find(String tenantId, String commandId) {
        return blocking("find Command effect", () -> latest(tenantId, commandId).map(StoredRevision::record));
    }

    @Override
    public Uni<CommandEffectRecord> createPending(CommandRequest<?> request, long nowEpochMs) {
        Objects.requireNonNull(request, "command request must not be null");
        return blocking("create pending Command effect", () -> {
            CommandEffectRecord pending = pending(request, nowEpochMs);
            StoredRevision initial = new StoredRevision(
                0L,
                pending,
                request.descriptor().inputType(),
                request.descriptor().outputType());
            append(initial, "Command effect already exists for commandId " + request.commandId());
            return pending;
        });
    }

    @Override
    public boolean supportsRetryAttempts() {
        return true;
    }

    @Override
    public Uni<CommandEffectRecord> createRetryAttempt(CommandRequest<?> request, long nowEpochMs) {
        Objects.requireNonNull(request, "command request must not be null");
        return transition(
            request.executionContext().tenantId(),
            request.commandId(),
            current -> {
                if (!current.inputDeclaredType().equals(request.descriptor().inputType())
                    || !current.outputDeclaredType().equals(request.descriptor().outputType())) {
                    throw new IllegalArgumentException(
                        "Retry request types do not match the recorded Command effect " + request.commandId());
                }
                return current.record().appendRetryAttempt(request, nowEpochMs);
            });
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(
        String tenantId,
        String commandId,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> current.record().dispatching(
            current.record().currentAttempt().attemptId(), nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDispatching(
        String tenantId,
        String commandId,
        String attemptId,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> current.record().dispatching(attemptId, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        Object output,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> {
            CommandEffectRecord source = legacyCompletionSource(current.record(), nowEpochMs);
            return source.succeeded(source.currentAttempt().attemptId(), output, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        String attemptId,
        Object output,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId,
            current -> current.record().succeeded(attemptId, output, nowEpochMs));
    }

    @Override
    public boolean supportsNativeOutcomeSnapshots() {
        return true;
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        Object output,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> {
            CommandEffectRecord source = legacyCompletionSource(current.record(), nowEpochMs);
            return source.succeeded(source.currentAttempt().attemptId(), output, outcome, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markSucceeded(
        String tenantId,
        String commandId,
        String attemptId,
        Object output,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId,
            current -> current.record().succeeded(attemptId, output, outcome, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(
        String tenantId,
        String commandId,
        Throwable failure,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> {
            CommandEffectRecord source = legacyCompletionSource(current.record(), nowEpochMs);
            return source.failed(source.currentAttempt().attemptId(), failure, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markFailed(
        String tenantId,
        String commandId,
        String attemptId,
        Throwable failure,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId,
            current -> current.record().failed(attemptId, failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(
        String tenantId,
        String commandId,
        Throwable failure,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> {
            CommandEffectRecord source = legacyCompletionSource(current.record(), nowEpochMs);
            return source.dlq(source.currentAttempt().attemptId(), failure, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markDlq(
        String tenantId,
        String commandId,
        String attemptId,
        Throwable failure,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId,
            current -> current.record().dlq(attemptId, failure, nowEpochMs));
    }

    @Override
    public Uni<CommandEffectRecord> markOutcome(
        String tenantId,
        String commandId,
        CommandEffectStatus status,
        Throwable failure,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId, current -> {
            CommandEffectRecord source = legacyCompletionSource(current.record(), nowEpochMs);
            return source.failedWithStatus(
                source.currentAttempt().attemptId(), status, failure, outcome, nowEpochMs);
        });
    }

    @Override
    public Uni<CommandEffectRecord> markOutcome(
        String tenantId,
        String commandId,
        String attemptId,
        CommandEffectStatus status,
        Throwable failure,
        CommandOutcomeSnapshot outcome,
        long nowEpochMs
    ) {
        return transition(tenantId, commandId,
            current -> current.record().failedWithStatus(attemptId, status, failure, outcome, nowEpochMs));
    }

    private Uni<CommandEffectRecord> transition(
        String tenantId,
        String commandId,
        Function<StoredRevision, CommandEffectRecord> transition
    ) {
        return blocking("transition Command effect", () -> {
            StoredRevision current = latest(tenantId, commandId)
                .orElseThrow(() -> new CommandEffectStoreException(
                    "No Command effect record found for commandId " + commandId));
            CommandEffectRecord updated;
            try {
                updated = transition.apply(current);
            } catch (IllegalArgumentException | IllegalStateException failure) {
                throw new CommandEffectConflictException(
                    "Illegal or stale Command effect transition for commandId " + commandId,
                    failure);
            }
            StoredRevision next = new StoredRevision(
                Math.incrementExact(current.revision()),
                updated,
                current.inputDeclaredType(),
                current.outputDeclaredType());
            append(next, "Concurrent Command effect transition for commandId " + commandId);
            return updated;
        });
    }

    private Optional<StoredRevision> latest(String tenantId, String commandId) {
        requireText(tenantId, "tenantId");
        requireText(commandId, "commandId");
        String key = commandKey(tenantId, commandId);
        try {
            List<Map<String, AttributeValue>> items = client.query(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#commandKey = :commandKey")
                .expressionAttributeNames(Map.of("#commandKey", COMMAND_KEY))
                .expressionAttributeValues(Map.of(":commandKey", stringValue(key)))
                .consistentRead(true)
                .scanIndexForward(false)
                .limit(1)
                .build()).items();
            if (items.isEmpty()) {
                return Optional.empty();
            }
            Map<String, AttributeValue> item = items.getFirst();
            long revision = longValue(item, REVISION);
            int schemaVersion = Math.toIntExact(longValue(item, SCHEMA_VERSION));
            if (schemaVersion != CommandEffectRecordCodec.SCHEMA_VERSION) {
                throw new CommandEffectStoreException(
                    "Unsupported durable Command effect item schema version " + schemaVersion);
            }
            CommandEffectRecordCodec.DecodedSnapshot decoded = codec.decode(stringValue(item, RECORD_JSON));
            if (!tenantId.equals(decoded.record().tenantId()) || !commandId.equals(decoded.record().commandId())) {
                throw new CommandEffectStoreException(
                    "Durable Command effect key does not match its encoded authority record");
            }
            return Optional.of(new StoredRevision(
                revision,
                decoded.record(),
                decoded.inputDeclaredType(),
                decoded.outputDeclaredType()));
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (DynamoDbException failure) {
            throw new CommandEffectStoreException(
                "Failed reading Command effect " + commandId + " from DynamoDB", failure);
        }
    }

    private void append(StoredRevision revision, String conflictMessage) {
        String encoded = codec.encode(
            revision.record(), revision.inputDeclaredType(), revision.outputDeclaredType());
        int recordBytes = encoded.getBytes(StandardCharsets.UTF_8).length;
        if (recordBytes > MAX_RECORD_BYTES) {
            throw new CommandEffectStoreException(
                "Durable Command effect record is " + recordBytes + " bytes; maximum is "
                    + MAX_RECORD_BYTES + ". Carry large content with PayloadReference.");
        }
        Map<String, AttributeValue> item = Map.of(
            COMMAND_KEY, stringValue(commandKey(revision.record().tenantId(), revision.record().commandId())),
            REVISION, numberValue(revision.revision()),
            SCHEMA_VERSION, numberValue(CommandEffectRecordCodec.SCHEMA_VERSION),
            RECORD_JSON, stringValue(encoded));
        try {
            client.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(#commandKey) AND attribute_not_exists(#revision)")
                .expressionAttributeNames(Map.of(
                    "#commandKey", COMMAND_KEY,
                    "#revision", REVISION))
                .build());
        } catch (ConditionalCheckFailedException failure) {
            throw new CommandEffectConflictException(conflictMessage, failure);
        } catch (DynamoDbException failure) {
            throw new CommandEffectStoreException(
                "Failed writing Command effect " + revision.record().commandId() + " to DynamoDB", failure);
        }
    }

    private <T> Uni<T> blocking(String operation, Supplier<T> supplier) {
        return Uni.createFrom().item(() -> {
            try {
                return supplier.get();
            } catch (CommandEffectStoreException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw new CommandEffectStoreException("Failed to " + operation, failure);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private static CommandEffectRecord pending(CommandRequest<?> request, long nowEpochMs) {
        return new CommandEffectRecord(
            request.executionContext().tenantId(),
            request.executionContext().executionId(),
            request.descriptor().stepId(),
            request.descriptor().command(),
            request.commandId(),
            CommandEffectStatus.PENDING,
            request.input(),
            null,
            null,
            null,
            Optional.empty(),
            List.of(new CommandEffectAttemptRecord(
                request.attemptId(),
                1,
                request.executionContext().executionId(),
                CommandEffectStatus.PENDING,
                null,
                null,
                Optional.empty(),
                nowEpochMs,
                nowEpochMs)),
            nowEpochMs,
            nowEpochMs);
    }

    private static CommandEffectRecord legacyCompletionSource(CommandEffectRecord record, long nowEpochMs) {
        return record.status() == CommandEffectStatus.PENDING
            ? record.dispatching(record.currentAttempt().attemptId(), nowEpochMs)
            : record;
    }

    private static String commandKey(String tenantId, String commandId) {
        return encodeKeyPart(tenantId) + "." + encodeKeyPart(commandId);
    }

    private static String encodeKeyPart(String value) {
        requireText(value, "Command effect key component");
        return KEY_ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static AttributeValue stringValue(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue numberValue(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String stringValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.s() == null) {
            throw new CommandEffectStoreException("Durable Command effect item is missing " + name);
        }
        return value.s();
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) {
            throw new CommandEffectStoreException("Durable Command effect item is missing " + name);
        }
        try {
            return Long.parseLong(value.n());
        } catch (NumberFormatException failure) {
            throw new CommandEffectStoreException(
                "Durable Command effect item has invalid " + name, failure);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new CommandEffectStoreException(field + " must not be blank");
        }
    }

    private record StoredRevision(
        long revision,
        CommandEffectRecord record,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }
}
