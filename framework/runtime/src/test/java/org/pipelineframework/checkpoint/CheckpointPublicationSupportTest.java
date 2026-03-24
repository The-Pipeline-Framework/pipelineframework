package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

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

    record PublishedOrder(String orderId, String customerId, String status) {
    }
}
