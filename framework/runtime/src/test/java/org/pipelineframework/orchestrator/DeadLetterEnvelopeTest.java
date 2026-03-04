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
            "transition1",
            "TimeoutException",
            "Request timed out after 30 seconds",
            timestamp);

        assertEquals("tenant1", envelope.tenantId());
        assertEquals("exec1", envelope.executionId());
        assertEquals("transition1", envelope.transitionKey());
        assertEquals("TimeoutException", envelope.errorCode());
        assertEquals("Request timed out after 30 seconds", envelope.errorMessage());
        assertEquals(timestamp, envelope.createdAtEpochMs());
    }

    @Test
    void handlesNullErrorMessage() {
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant2",
            "exec2",
            "transition2",
            "NullPointerException",
            null,
            1234567890L);

        assertNull(envelope.errorMessage());
        assertEquals("NullPointerException", envelope.errorCode());
    }

    @Test
    void handlesLongErrorMessage() {
        String longMessage = "A".repeat(5000);
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant3",
            "exec3",
            "transition3",
            "CustomException",
            longMessage,
            9876543210L);

        assertEquals(longMessage, envelope.errorMessage());
        assertEquals(5000, envelope.errorMessage().length());
    }

    @Test
    void preservesTransitionKey() {
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant4",
            "exec4",
            "exec4:2:5",
            "RetryExhaustedException",
            "Maximum retries exceeded",
            1111111111L);

        assertEquals("exec4:2:5", envelope.transitionKey());
    }
}