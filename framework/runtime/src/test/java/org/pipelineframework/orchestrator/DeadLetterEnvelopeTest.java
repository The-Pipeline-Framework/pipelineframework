package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterEnvelopeTest {

    @Test
    void buildThrowsWhenRequiredMissing() {
        assertThrows(NullPointerException.class, () ->
            DeadLetterEnvelope.builder()
                .executionId("x")
                .build());
    }

    @Test
    void createsEnvelopeWithAllFields() {
        long timestamp = System.currentTimeMillis();
        DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
            .tenantId("tenant1")
            .executionId("exec1")
            .executionKey("tenant1:exec1:key")
            .correlationId("corr-1")
            .transitionKey("transition1")
            .resourceType("tpf.orchestrator.execution")
            .resourceName("OrchestratorService/Run")
            .transport("REST")
            .platform("FUNCTION")
            .terminalStatus("FAILED")
            .terminalReason("retry_exhausted")
            .errorCode("TimeoutException")
            .errorMessage("Request timed out after 30 seconds")
            .retryable(true)
            .retriesObserved(3)
            .createdAtEpochMs(timestamp)
            .build();

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
        DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
            .tenantId("tenant2")
            .executionId("exec2")
            .executionKey("tenant2:exec2:key")
            .correlationId("corr-2")
            .transitionKey("transition2")
            .resourceType("tpf.orchestrator.execution")
            .resourceName("OrchestratorService/Run")
            .transport("GRPC")
            .platform("COMPUTE")
            .terminalStatus("FAILED")
            .terminalReason("non_retryable")
            .errorCode("NullPointerException")
            .errorMessage(null)
            .retryable(false)
            .retriesObserved(0)
            .createdAtEpochMs(1234567890L)
            .build();

        assertNull(envelope.errorMessage());
        assertEquals("NullPointerException", envelope.errorCode());
        assertFalse(envelope.retryable());
        assertEquals("non_retryable", envelope.terminalReason());
    }

    @Test
    void handlesLongErrorMessage() {
        String longMessage = "A".repeat(5000);
        DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
            .tenantId("tenant3")
            .executionId("exec3")
            .executionKey("tenant3:exec3:key")
            .correlationId("corr-3")
            .transitionKey("transition3")
            .resourceType("tpf.orchestrator.execution")
            .resourceName("OrchestratorService/Run")
            .transport("LOCAL")
            .platform("COMPUTE")
            .terminalStatus("FAILED")
            .terminalReason("retry_exhausted")
            .errorCode("CustomException")
            .errorMessage(longMessage)
            .retryable(true)
            .retriesObserved(1)
            .createdAtEpochMs(9876543210L)
            .build();

        assertEquals(longMessage, envelope.errorMessage());
        assertEquals(5000, envelope.errorMessage().length());
    }

    @Test
    void preservesTransitionKey() {
        DeadLetterEnvelope envelope = DeadLetterEnvelope.builder()
            .tenantId("tenant4")
            .executionId("exec4")
            .executionKey("tenant4:exec4:key")
            .correlationId("corr-4")
            .transitionKey("exec4:2:5")
            .resourceType("tpf.orchestrator.execution")
            .resourceName("OrchestratorService/Run")
            .transport("FUNCTION")
            .platform("FUNCTION")
            .terminalStatus("FAILED")
            .terminalReason("retry_exhausted")
            .errorCode("RetryExhaustedException")
            .errorMessage("Maximum retries exceeded")
            .retryable(true)
            .retriesObserved(5)
            .createdAtEpochMs(1111111111L)
            .build();

        assertEquals("exec4:2:5", envelope.transitionKey());
    }

    @Test
    void retriesObservedEdgeValues() {
        DeadLetterEnvelope zeroRetries = DeadLetterEnvelope.builder()
            .tenantId("tenant5")
            .executionId("exec5")
            .transitionKey("transition5")
            .errorCode("Error")
            .retriesObserved(0)
            .createdAtEpochMs(1L)
            .build();
        assertEquals(0, zeroRetries.retriesObserved());

        assertThrows(IllegalArgumentException.class, () ->
            DeadLetterEnvelope.builder()
                .tenantId("tenant5")
                .executionId("exec5")
                .transitionKey("transition5")
                .errorCode("Error")
                .retriesObserved(-1)
                .createdAtEpochMs(1L)
                .build());

        int largeRetries = Integer.MAX_VALUE;
        DeadLetterEnvelope largeRetryEnvelope = DeadLetterEnvelope.builder()
            .tenantId("tenant5")
            .executionId("exec5")
            .transitionKey("transition5")
            .errorCode("Error")
            .retriesObserved(largeRetries)
            .createdAtEpochMs(1L)
            .build();
        assertEquals(largeRetries, largeRetryEnvelope.retriesObserved());
    }

    @Test
    void emptyVsNullErrorMessage() {
        DeadLetterEnvelope emptyErrorMessage = DeadLetterEnvelope.builder()
            .tenantId("tenant6")
            .executionId("exec6")
            .transitionKey("transition6")
            .errorCode("Error")
            .errorMessage("")
            .retriesObserved(0)
            .createdAtEpochMs(1L)
            .build();
        assertEquals("", emptyErrorMessage.errorMessage());

        DeadLetterEnvelope nullErrorMessage = DeadLetterEnvelope.builder()
            .tenantId("tenant6")
            .executionId("exec6")
            .transitionKey("transition6")
            .errorCode("Error")
            .errorMessage(null)
            .retriesObserved(0)
            .createdAtEpochMs(1L)
            .build();
        assertNull(nullErrorMessage.errorMessage());
    }

    @Test
    void deprecatedConstructorValidatesRequiredFields() {
        assertThrows(NullPointerException.class, () ->
            new DeadLetterEnvelope(
                null,
                "exec",
                "exec-key",
                "corr",
                "transition",
                "resource",
                "resourceName",
                "REST",
                "COMPUTE",
                "FAILED",
                "retry_exhausted",
                "Error",
                "msg",
                true,
                0,
                1L));
    }
}
