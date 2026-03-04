package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CreateExecutionResultTest {

    @Test
    void createsResultWithRecordAndDuplicateFlag() {
        ExecutionRecord record = new ExecutionRecord(
            "tenant1",
            "exec1",
            "key1",
            ExecutionStatus.PENDING,
            0,
            1,
            1,
            null,
            null,
            null,
            null,
            0L,
            1234567890L,
            7654321L);

        CreateExecutionResult result = new CreateExecutionResult(record, true);

        assertEquals(record, result.record());
        assertTrue(result.duplicate());
    }

    @Test
    void createsResultWithNonDuplicate() {
        ExecutionRecord record = new ExecutionRecord(
            "tenant2",
            "exec2",
            "key2",
            ExecutionStatus.RUNNING,
            0,
            1,
            1,
            null,
            null,
            null,
            null,
            0L,
            1234567890L,
            7654321L);

        CreateExecutionResult result = new CreateExecutionResult(record, false);

        assertEquals(record, result.record());
        assertFalse(result.duplicate());
    }

    @Test
    void recordClassAccessorsWork() {
        CreateExecutionResult result = new CreateExecutionResult(
            new ExecutionRecord(
                "tenant3",
                "exec3",
                "key3",
                ExecutionStatus.SUCCEEDED,
                2,
                5,
                1,
                null,
                "SUCCESS",
                "completed",
                null,
                0L,
                9876543210L,
                7654321L),
            false);

        assertEquals("tenant3", result.record().tenantId());
        assertEquals("exec3", result.record().executionId());
        assertEquals("key3", result.record().executionKey());
        assertEquals(ExecutionStatus.SUCCEEDED, result.record().status());
        assertEquals(2, result.record().currentStepIndex());
        assertEquals(5, result.record().attempt());
    }
}