package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * Immutable DynamoDB payload manifests and byte chunks for one execution state store.
 *
 * <p>The chunk size is a storage invariant, rather than a record-count heuristic. It leaves
 * substantial room below DynamoDB's 400 KiB item limit for keys and immutable metadata, so an
 * encoded payload is split before any execution-state write is attempted.</p>
 */
final class DynamoExecutionPayloadStore {
    static final int MAX_CHUNK_BYTES = 256 * 1024;

    private static final String PAYLOAD_ID = "payload_id";
    private static final String PAYLOAD_PART = "payload_part";
    private static final String MANIFEST_PART = "MANIFEST";
    private static final String CHUNK_PREFIX = "CHUNK#";
    private static final String PAYLOAD_BYTES = "payload_bytes";
    private static final String PAYLOAD_LENGTH_BYTES = "payload_length_bytes";
    private static final String CHUNK_COUNT = "chunk_count";
    private static final String SHA256 = "sha256";
    private static final String TTL_EPOCH_S = "ttl_epoch_s";

    private final Supplier<DynamoDbClient> client;
    private final Supplier<String> tableName;

    DynamoExecutionPayloadStore(Supplier<DynamoDbClient> client, Supplier<String> tableName) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
    }

    String write(String tenantId, String executionId, String slot, String payload, long ttlEpochS) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(executionId, "executionId must not be null");
        Objects.requireNonNull(slot, "slot must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        String contentHash = sha256(bytes);
        String payloadId = "p-" + sha256((tenantId + "\u0000" + executionId + "\u0000" + slot + "\u0000" + contentHash)
            .getBytes(StandardCharsets.UTF_8));
        int chunkCount = Math.max(1, (bytes.length + MAX_CHUNK_BYTES - 1) / MAX_CHUNK_BYTES);

        for (int index = 0; index < chunkCount; index++) {
            int offset = index * MAX_CHUNK_BYTES;
            int length = Math.min(MAX_CHUNK_BYTES, bytes.length - offset);
            byte[] chunk = new byte[length];
            System.arraycopy(bytes, offset, chunk, 0, length);
            putImmutable(payloadId, chunkPart(index), Map.of(
                PAYLOAD_ID, stringValue(payloadId),
                PAYLOAD_PART, stringValue(chunkPart(index)),
                PAYLOAD_BYTES, AttributeValue.builder().b(SdkBytes.fromByteArray(chunk)).build(),
                TTL_EPOCH_S, numberValue(ttlEpochS)));
        }

        putImmutable(payloadId, MANIFEST_PART, Map.of(
            PAYLOAD_ID, stringValue(payloadId),
            PAYLOAD_PART, stringValue(MANIFEST_PART),
            PAYLOAD_LENGTH_BYTES, numberValue(bytes.length),
            CHUNK_COUNT, numberValue(chunkCount),
            SHA256, stringValue(contentHash),
            TTL_EPOCH_S, numberValue(ttlEpochS)));
        return payloadId;
    }

    String read(String payloadId) {
        Map<String, AttributeValue> manifest = client.get().getItem(GetItemRequest.builder()
            .tableName(tableName.get())
            .key(payloadKey(payloadId, MANIFEST_PART))
            .consistentRead(true)
            .build()).item();
        if (manifest == null || manifest.isEmpty()) {
            throw new IllegalStateException("Execution payload manifest is missing: " + payloadId);
        }
        int expectedChunks = intValue(manifest, CHUNK_COUNT);
        int expectedLength = intValue(manifest, PAYLOAD_LENGTH_BYTES);
        String expectedHash = string(manifest, SHA256);
        List<Map<String, AttributeValue>> chunks = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = Map.of();
        do {
            QueryRequest.Builder query = QueryRequest.builder()
                .tableName(tableName.get())
                .keyConditionExpression("#payloadId = :payloadId AND begins_with(#payloadPart, :chunkPrefix)")
                .expressionAttributeNames(Map.of("#payloadId", PAYLOAD_ID, "#payloadPart", PAYLOAD_PART))
                .expressionAttributeValues(Map.of(
                    ":payloadId", stringValue(payloadId),
                    ":chunkPrefix", stringValue(CHUNK_PREFIX)))
                .consistentRead(true);
            if (!lastEvaluatedKey.isEmpty()) {
                query.exclusiveStartKey(lastEvaluatedKey);
            }
            var response = client.get().query(query.build());
            chunks.addAll(response.items());
            lastEvaluatedKey = response.lastEvaluatedKey();
        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        chunks.sort(Comparator.comparing(item -> string(item, PAYLOAD_PART)));
        if (chunks.size() != expectedChunks) {
            throw new IllegalStateException("Execution payload chunk count is incomplete for " + payloadId);
        }
        byte[] bytes = new byte[expectedLength];
        int offset = 0;
        for (int index = 0; index < chunks.size(); index++) {
            Map<String, AttributeValue> chunk = chunks.get(index);
            if (!chunkPart(index).equals(string(chunk, PAYLOAD_PART))) {
                throw new IllegalStateException("Execution payload chunk order is invalid for " + payloadId);
            }
            byte[] value = chunk.get(PAYLOAD_BYTES).b().asByteArray();
            if (offset + value.length > bytes.length) {
                throw new IllegalStateException("Execution payload length is invalid for " + payloadId);
            }
            System.arraycopy(value, 0, bytes, offset, value.length);
            offset += value.length;
        }
        if (offset != expectedLength || !expectedHash.equals(sha256(bytes))) {
            throw new IllegalStateException("Execution payload checksum is invalid for " + payloadId);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void putImmutable(String payloadId, String part, Map<String, AttributeValue> item) {
        try {
            client.get().putItem(PutItemRequest.builder()
                .tableName(tableName.get())
                .item(item)
                .conditionExpression("attribute_not_exists(#payloadId) AND attribute_not_exists(#payloadPart)")
                .expressionAttributeNames(Map.of("#payloadId", PAYLOAD_ID, "#payloadPart", PAYLOAD_PART))
                .build());
        } catch (ConditionalCheckFailedException collision) {
            Map<String, AttributeValue> existing = client.get().getItem(GetItemRequest.builder()
                .tableName(tableName.get())
                .key(payloadKey(payloadId, part))
                .consistentRead(true)
                .build()).item();
            if (!payloadAttributesMatch(item, existing)) {
                throw new IllegalStateException("Execution payload collision contains different data: " + payloadId, collision);
            }
        }
    }

    private static boolean payloadAttributesMatch(
        Map<String, AttributeValue> expected,
        Map<String, AttributeValue> existing
    ) {
        if (existing == null) {
            return false;
        }
        Map<String, AttributeValue> expectedWithoutTtl = new java.util.HashMap<>(expected);
        expectedWithoutTtl.remove(TTL_EPOCH_S);
        Map<String, AttributeValue> existingWithoutTtl = new java.util.HashMap<>(existing);
        existingWithoutTtl.remove(TTL_EPOCH_S);
        return expectedWithoutTtl.equals(existingWithoutTtl);
    }

    private static Map<String, AttributeValue> payloadKey(String payloadId, String part) {
        return Map.of(PAYLOAD_ID, stringValue(payloadId), PAYLOAD_PART, stringValue(part));
    }

    private static String chunkPart(int index) {
        return CHUNK_PREFIX + String.format("%08d", index);
    }

    private static AttributeValue stringValue(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue numberValue(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static int intValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) {
            throw new IllegalStateException("Execution payload manifest is missing " + name);
        }
        try {
            return Integer.parseInt(value.n());
        } catch (NumberFormatException failure) {
            throw new IllegalStateException("Execution payload manifest has an invalid " + name, failure);
        }
    }

    private static String string(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.s() == null) {
            throw new IllegalStateException("Execution payload is missing " + name);
        }
        return value.s();
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
