package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterEnvelopeTest {

    @Test
    void createsEnvelopeWithAllFields() {
        long timestamp = System.currentTimeMillis();
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant1",
            "exec1",
            "tenant1:exec1:key",
            "corr-1",
            "transition1",
            "tpf.orchestrator.execution",
            "OrchestratorService/Run",
            "REST",
            "FUNCTION",
            "FAILED",
            "retry_exhausted",
            "TimeoutException",
            "Request timed out after 30 seconds",
            true,
            3,
            timestamp);

        assertEquals("tenant1", envelope.tenantId());
        assertEquals("exec1", envelope.executionId());
        assertEquals("tenant1:exec1:key", envelope.executionKey());
        assertEquals("corr-1", envelope.correlationId());
        assertEquals("transition1", envelope.transitionKey());
        assertEquals("tpf.orchestrator.execution", envelope.resourceType());
        assertEquals("OrchestratorService/Run", envelope.resourceName());
        assertEquals("REST", envelope.transport());
        assertEquals("FUNCTION", envelope.platform());
        assertEquals("FAILED", envelope.terminalStatus());
        assertEquals("retry_exhausted", envelope.terminalReason());
        assertEquals("TimeoutException", envelope.errorCode());
        assertEquals("Request timed out after 30 seconds", envelope.errorMessage());
        assertTrue(envelope.retryable());
        assertEquals(3, envelope.retriesObserved());
        assertEquals(timestamp, envelope.createdAtEpochMs());
    }

    @Test
    void handlesNullErrorMessage() {
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant2",
            "exec2",
            "tenant2:exec2:key",
            "corr-2",
            "transition2",
            "tpf.orchestrator.execution",
            "OrchestratorService/Run",
            "GRPC",
            "COMPUTE",
            "FAILED",
            "non_retryable",
            "NullPointerException",
            null,
            false,
            0,
            1234567890L);

        assertNull(envelope.errorMessage());
        assertEquals("NullPointerException", envelope.errorCode());
        assertFalse(envelope.retryable());
        assertEquals("non_retryable", envelope.terminalReason());
    }

    @Test
    void handlesLongErrorMessage() {
        String longMessage = "A".repeat(5000);
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant3",
            "exec3",
            "tenant3:exec3:key",
            "corr-3",
            "transition3",
            "tpf.orchestrator.execution",
            "OrchestratorService/Run",
            "LOCAL",
            "COMPUTE",
            "FAILED",
            "retry_exhausted",
            "CustomException",
            longMessage,
            true,
            1,
            9876543210L);

        assertEquals(longMessage, envelope.errorMessage());
        assertEquals(5000, envelope.errorMessage().length());
    }

    @Test
    void preservesTransitionKey() {
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant4",
            "exec4",
            "tenant4:exec4:key",
            "corr-4",
            "exec4:2:5",
            "tpf.orchestrator.execution",
            "OrchestratorService/Run",
            "FUNCTION",
            "FUNCTION",
            "FAILED",
            "retry_exhausted",
            "RetryExhaustedException",
            "Maximum retries exceeded",
            true,
            5,
            1111111111L);

        assertEquals("exec4:2:5", envelope.transitionKey());
    }
}
