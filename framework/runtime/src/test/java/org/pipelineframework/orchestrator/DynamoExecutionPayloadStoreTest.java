package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

class DynamoExecutionPayloadStoreTest {

    @Test
    void writesSerializedBytesInBoundedChunksBeforeTheManifest() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionPayloadStore store = new DynamoExecutionPayloadStore(() -> client, () -> "tpf_execution_payload");
        String payload = "x".repeat(DynamoExecutionPayloadStore.MAX_CHUNK_BYTES * 2 + 37);

        String reference = store.write("tenant-a", "exec-1", "result:step-1", payload, 1234L);

        ArgumentCaptor<PutItemRequest> requests = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(client, times(4)).putItem(requests.capture());
        List<PutItemRequest> writes = requests.getAllValues();
        assertTrue(writes.get(writes.size() - 1).item().containsKey("chunk_count"));
        writes.subList(0, writes.size() - 1).forEach(request -> assertTrue(
            request.item().get("payload_bytes").b().asByteArray().length <= DynamoExecutionPayloadStore.MAX_CHUNK_BYTES));
        assertTrue(reference.startsWith("p-"));
    }

    @Test
    void readsAndChecksTheManifestOrderedChunks() throws Exception {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionPayloadStore store = new DynamoExecutionPayloadStore(() -> client, () -> "tpf_execution_payload");
        String payload = "a".repeat(DynamoExecutionPayloadStore.MAX_CHUNK_BYTES) + "tail";
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] first = java.util.Arrays.copyOfRange(bytes, 0, DynamoExecutionPayloadStore.MAX_CHUNK_BYTES);
        byte[] second = java.util.Arrays.copyOfRange(bytes, DynamoExecutionPayloadStore.MAX_CHUNK_BYTES, bytes.length);
        Map<String, AttributeValue> manifest = Map.of(
            "payload_id", string("p-1"),
            "payload_part", string("MANIFEST"),
            "payload_length_bytes", number(bytes.length),
            "chunk_count", number(2),
            "sha256", string(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))),
            "ttl_epoch_s", number(1234));
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(manifest).build());
        when(client.query(any(QueryRequest.class))).thenReturn(
            QueryResponse.builder()
                .items(List.of(chunk("p-1", "CHUNK#00000000", first)))
                .lastEvaluatedKey(Map.of("payload_id", string("p-1"), "payload_part", string("CHUNK#00000000")))
                .build(),
            QueryResponse.builder()
                .items(List.of(chunk("p-1", "CHUNK#00000001", second)))
                .build());

        assertEquals(payload, store.read("p-1"));
    }

    @Test
    void acceptsAnExistingImmutablePayloadWhenOnlyItsTtlDiffers() throws Exception {
        DynamoDbClient client = mock(DynamoDbClient.class);
        DynamoExecutionPayloadStore store = new DynamoExecutionPayloadStore(() -> client, () -> "tpf_execution_payload");
        String payload = "payload";
        String contentHash = sha256(payload.getBytes(StandardCharsets.UTF_8));
        String payloadId = "p-" + sha256(("tenant-a\u0000exec-1\u0000result:step-1\u0000" + contentHash)
            .getBytes(StandardCharsets.UTF_8));
        Map<String, AttributeValue> existing = Map.of(
            "payload_id", string(payloadId),
            "payload_part", string("CHUNK#00000000"),
            "payload_bytes", AttributeValue.builder().b(SdkBytes.fromUtf8String(payload)).build(),
            "ttl_epoch_s", number(99));
        when(client.putItem(any(PutItemRequest.class))).thenThrow(ConditionalCheckFailedException.builder().build())
            .thenReturn(PutItemResponse.builder().build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(existing).build());

        assertEquals(payloadId, store.write("tenant-a", "exec-1", "result:step-1", payload, 1234));
        verify(client, times(2)).putItem(any(PutItemRequest.class));
    }

    private static Map<String, AttributeValue> chunk(String payloadId, String part, byte[] bytes) {
        return Map.of(
            "payload_id", string(payloadId),
            "payload_part", string(part),
            "payload_bytes", AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build(),
            "ttl_epoch_s", number(1234));
    }

    private static AttributeValue string(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
