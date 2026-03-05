package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateExecutionResultTest {

    @Test
    void createsResultWithRecordAndDuplicateFlag() {
        ExecutionRecord<Object, Object> record = createRecord("tenant-1", "exec-1", ExecutionStatus.QUEUED);

        CreateExecutionResult result = new CreateExecutionResult(record, true);

        assertEquals(record, result.record());
        assertTrue(result.duplicate());
    }

    @Test
    void createsResultWithNonDuplicate() {
        ExecutionRecord<Object, Object> record = createRecord("tenant-2", "exec-2", ExecutionStatus.RUNNING);

        CreateExecutionResult result = new CreateExecutionResult(record, false);

        assertEquals(record, result.record());
        assertFalse(result.duplicate());
    }

    @Test
    void recordClassAccessorsWork() {
        CreateExecutionResult result = new CreateExecutionResult(
            createRecord("tenant-3", "exec-3", ExecutionStatus.SUCCEEDED),
            false);

        assertEquals("tenant-3", result.record().tenantId());
        assertEquals("exec-3", result.record().executionId());
        assertEquals("key-exec-3", result.record().executionKey());
        assertEquals(ExecutionStatus.SUCCEEDED, result.record().status());
    }

    private static ExecutionRecord<Object, Object> createRecord(
        String tenantId,
        String executionId,
        ExecutionStatus status) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            "key-" + executionId,
            status,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            null,
            null,
            null,
            null,
            123L,
            456L,
            789L);
    }
}
