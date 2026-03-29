package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;

class CheckpointPublicationSupportTest {

    @Test
    void preservesExistingIdempotencyKeyWhenPresent() {
        String key = CheckpointPublicationSupport.deriveIdempotencyKey(
            " existing-key ",
            List.of("orderId", "customerId"),
            new PublishedOrder("o-1", "c-1", "ready"));

        assertEquals("existing-key", key);
    }

    @Test
    void derivesDeterministicKeyFromConfiguredFields() {
        String key = CheckpointPublicationSupport.deriveIdempotencyKey(
            null,
            List.of("orderId", "customerId", "status"),
            new PublishedOrder("o-1", "c-1", "ready"));

        assertEquals("o-1:c-1:ready", key);
    }

    @Test
    void returnsNullWhenAnyConfiguredFieldIsMissing() {
        String key = CheckpointPublicationSupport.deriveIdempotencyKey(
            null,
            List.of("orderId", "missingField"),
            new PublishedOrder("o-1", "c-1", "ready"));

        assertNull(key);
    }

    @Test
    void normalizesProtobufPayloadsToExpectedDomainType() {
        CheckpointPublishRequest request = CheckpointPublishRequest.newBuilder()
            .setPublication("tpfgo.test.v1")
            .setTenantId("default")
            .setIdempotencyKey("key-1")
            .build();
        PublishedCheckpoint normalized = CheckpointPublicationSupport.normalizePayload(
            request,
            PublishedCheckpoint.class);

        assertEquals("tpfgo.test.v1", normalized.publication());
        assertEquals("default", normalized.tenantId());
        assertEquals("key-1", normalized.idempotencyKey());
    }

    @Test
    void returnsDomainPayloadUnchangedWhenAlreadyExpectedType() {
        PublishedCheckpoint payload = new PublishedCheckpoint("pub", "tenant", "key");
        Object normalized = CheckpointPublicationSupport.normalizePayload(payload, PublishedCheckpoint.class);

        assertInstanceOf(PublishedCheckpoint.class, normalized);
        assertEquals(payload, normalized);
    }

    record PublishedOrder(String orderId, String customerId, String status) {
    }

    record PublishedCheckpoint(String publication, String tenantId, String idempotencyKey) {
    }
}
