package org.pipelineframework.orchestrator;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsWorkDispatcherTest {

    private SqsWorkDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new SqsWorkDispatcher();
    }

    @Test
    void providerNameIsSqs() {
        assertEquals("sqs", dispatcher.providerName());
    }

    @Test
    void priorityIsNegative() {
        assertEquals(-1000, dispatcher.priority());
    }

    @Test
    void enqueueNowThrowsUnsupportedOperation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueNow(item).await().indefinitely());
    }

    @Test
    void enqueueDelayedThrowsUnsupportedOperation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofSeconds(10);

        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueDelayed(item, delay).await().indefinitely());
    }

    @Test
    void enqueueDelayedWithNullDelayThrowsUnsupportedOperation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueDelayed(item, null).await().indefinitely());
    }

    @Test
    void errorMessageIndicatesMissingImplementation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        try {
            dispatcher.enqueueNow(item).await().indefinitely();
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("SqsWorkDispatcher"));
            assertTrue(e.getMessage().contains("not implemented"));
            assertTrue(e.getMessage().contains("deployment-specific provider module"));
        }
    }

    @Test
    void enqueueDelayedErrorMessageIndicatesMissingImplementation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        try {
            dispatcher.enqueueDelayed(item, Duration.ofMinutes(5)).await().indefinitely();
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("SqsWorkDispatcher"));
            assertTrue(e.getMessage().contains("not implemented"));
        }
    }

    @Test
    void enqueueNowWithNullItemThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueNow(null).await().indefinitely());
    }
}