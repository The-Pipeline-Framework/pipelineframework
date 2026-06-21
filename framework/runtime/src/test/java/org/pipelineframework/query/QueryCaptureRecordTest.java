package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class QueryCaptureRecordTest {

    private static final Instant FIXED_TIME = Instant.parse("2024-01-01T00:00:00Z");

    @Test
    void constructsRecordWithAllRequiredFields() {
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant-1",
            "exec-abc",
            2,
            "customer-risk-by-id",
            "v1",
            "tenant-1:exec-abc:2:cust-123",
            "{\"customerId\":\"cust-123\"}",
            "{\"riskScore\":42}",
            "com.example.CustomerRiskSnapshot",
            FIXED_TIME);

        assertEquals("tenant-1", record.tenantId());
        assertEquals("exec-abc", record.executionId());
        assertEquals(2, record.stepIndex());
        assertEquals("customer-risk-by-id", record.queryId());
        assertEquals("v1", record.queryVersion());
        assertEquals("tenant-1:exec-abc:2:cust-123", record.captureKey());
        assertEquals("{\"customerId\":\"cust-123\"}", record.inputJson());
        assertEquals("{\"riskScore\":42}", record.outputJson());
        assertEquals("com.example.CustomerRiskSnapshot", record.outputType());
        assertEquals(FIXED_TIME, record.capturedAt());
    }

    @Test
    void defaultsCapturedAtToNowWhenNull() {
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant-1", "exec-abc", 0,
            "query-id", "v1", "capture-key",
            null, "{}", "com.example.Type", null);

        assertNotNull(record.capturedAt());
    }

    @Test
    void acceptsNullInputJson() {
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant-1", "exec-abc", 0,
            "query-id", "v1", "capture-key",
            null, "{\"result\":\"ok\"}", "com.example.Type", FIXED_TIME);

        assertEquals(null, record.inputJson());
        assertEquals("{\"result\":\"ok\"}", record.outputJson());
    }

    @Test
    void rejectsNullTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord(null, "exec-abc", 0, "qid", "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("   ", "exec-abc", 0, "qid", "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsNullExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", null, 0, "qid", "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsBlankExecutionId() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "  ", 0, "qid", "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsNegativeStepIndex() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", -1, "qid", "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void acceptsZeroStepIndex() {
        QueryCaptureRecord record = new QueryCaptureRecord(
            "tenant", "exec-abc", 0, "qid", "v1", "key", null, "{}", "Type", FIXED_TIME);

        assertEquals(0, record.stepIndex());
    }

    @Test
    void rejectsNullQueryId() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, null, "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsBlankQueryId() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "  ", "v1", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsNullQueryVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", null, "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsBlankQueryVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "  ", "key", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsNullCaptureKey() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "v1", null, null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsBlankCaptureKey() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "v1", "  ", null, "{}", "Type", FIXED_TIME));
    }

    @Test
    void rejectsNullOutputJson() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "v1", "key", null, null, "Type", FIXED_TIME));
    }

    @Test
    void rejectsBlankOutputJson() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "v1", "key", null, "  ", "Type", FIXED_TIME));
    }

    @Test
    void rejectsNullOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "v1", "key", null, "{}", null, FIXED_TIME));
    }

    @Test
    void rejectsBlankOutputType() {
        assertThrows(IllegalArgumentException.class, () ->
            new QueryCaptureRecord("tenant", "exec-abc", 0, "qid", "v1", "key", null, "{}", "  ", FIXED_TIME));
    }

    @Test
    void recordIsValueEquatable() {
        QueryCaptureRecord r1 = new QueryCaptureRecord(
            "t", "e", 0, "q", "v1", "k", null, "{}", "T", FIXED_TIME);
        QueryCaptureRecord r2 = new QueryCaptureRecord(
            "t", "e", 0, "q", "v1", "k", null, "{}", "T", FIXED_TIME);

        assertEquals(r1, r2);
        assertEquals(r1.hashCode(), r2.hashCode());
    }
}
