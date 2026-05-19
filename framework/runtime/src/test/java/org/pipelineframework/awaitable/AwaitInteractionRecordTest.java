package org.pipelineframework.awaitable;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitInteractionRecordTest {

    @Test
    void constructsWithAllRequiredFields() {
        AwaitInteractionRecord record = sampleRecord();

        assertEquals("tenant1", record.tenantId());
        assertEquals("exec-1", record.executionId());
        assertEquals("review", record.stepId());
        assertEquals(0, record.stepIndex());
        assertEquals("com.example.Output", record.outputType());
        assertEquals("interaction-id", record.interactionId());
        assertEquals("corr-id", record.correlationId());
        assertEquals("idem-key", record.idempotencyKey());
        assertEquals(0L, record.version());
        assertEquals(AwaitInteractionStatus.WAITING, record.status());
    }

    @Test
    void normalizesNullTransportMetadataToEmptyMap() {
        AwaitInteractionRecord record = new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "com.example.Output",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook",
            null, // null transportMetadata
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE);

        assertEquals(Map.of(), record.transportMetadata());
    }

    @Test
    void makesImmutableCopyOfTransportMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key", "value");

        AwaitInteractionRecord record = new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "com.example.Output",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook",
            metadata,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE);

        metadata.put("key2", "value2"); // mutate original
        assertEquals(1, record.transportMetadata().size()); // record not affected
        assertThrows(UnsupportedOperationException.class, () -> record.transportMetadata().put("k", "v"));
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "", "exec-1", "review", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            null, "exec-1", "review", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", " ", "review", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankStepId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsNegativeStepIndex() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", -1, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "  ",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankInteractionId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "Out.class",
            "", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankCorrelationId() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "Out.class",
            "interaction-id", "  ", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsBlankIdempotencyKey() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void rejectsNullStatus() {
        assertThrows(IllegalArgumentException.class, () -> new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, null,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE));
    }

    @Test
    void acceptsZeroStepIndex() {
        AwaitInteractionRecord record = new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "Out.class",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            null, null, null, null, null, "webhook", null,
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE);

        assertEquals(0, record.stepIndex());
    }

    private static AwaitInteractionRecord sampleRecord() {
        return new AwaitInteractionRecord(
            "tenant1", "exec-1", "review", 0, "com.example.Output",
            "interaction-id", "corr-id", "cause-id", "idem-key",
            0L, AwaitInteractionStatus.WAITING,
            "request", null, null, null, null, "webhook",
            Map.of("key", "value"),
            70_000L, 10_000L, 10_000L, Long.MAX_VALUE);
    }
}