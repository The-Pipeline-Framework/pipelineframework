package org.pipelineframework.reject;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InMemoryItemRejectSinkTest {

    @Test
    void startupValidationFailsForNonPositiveCapacity() {
        ItemRejectConfig config = mockConfig(0);
        InMemoryItemRejectSink sink = new InMemoryItemRejectSink();

        var validationError = sink.startupValidationError(config);

        assertTrue(validationError.isPresent());
        assertTrue(validationError.get().contains("memory-capacity"));
    }

    @Test
    void startupValidationPassesForPositiveCapacity() {
        ItemRejectConfig config = mockConfig(3);
        InMemoryItemRejectSink sink = new InMemoryItemRejectSink();

        var validationError = sink.startupValidationError(config);

        assertTrue(validationError.isEmpty());
    }

    @Test
    void retainsBoundedRingBuffer() {
        ItemRejectConfig config = mockConfig(2);
        InMemoryItemRejectSink sink = new InMemoryItemRejectSink();
        sink.itemRejectConfig = config;

        sink.publish(envelope("f1")).await().indefinitely();
        sink.publish(envelope("f2")).await().indefinitely();
        sink.publish(envelope("f3")).await().indefinitely();

        var snapshot = sink.snapshot();
        assertEquals(2, snapshot.size());
        assertEquals("f2", snapshot.get(0).itemFingerprint());
        assertEquals("f3", snapshot.get(1).itemFingerprint());
    }

    private static ItemRejectEnvelope envelope(String fingerprint) {
        return new ItemRejectEnvelope(
            null,
            "exec-1",
            "corr-1",
            "idem-1",
            "none",
            "com.example.Step",
            "Step",
            "ITEM",
            0,
            0,
            3,
            1,
            RuntimeException.class.getName(),
            "boom",
            System.currentTimeMillis(),
            fingerprint,
            null,
            null);
    }

    private static ItemRejectConfig mockConfig(int capacity) {
        ItemRejectConfig config = mock(ItemRejectConfig.class);
        ItemRejectConfig.SqsConfig sqs = mock(ItemRejectConfig.SqsConfig.class);
        when(config.provider()).thenReturn("memory");
        when(config.strictStartup()).thenReturn(true);
        when(config.includePayload()).thenReturn(false);
        when(config.publishFailurePolicy()).thenReturn(ItemRejectFailurePolicy.CONTINUE);
        when(config.memoryCapacity()).thenReturn(capacity);
        when(config.sqs()).thenReturn(sqs);
        when(sqs.queueUrl()).thenReturn(Optional.empty());
        when(sqs.region()).thenReturn(Optional.empty());
        when(sqs.endpointOverride()).thenReturn(Optional.empty());
        return config;
    }
}
