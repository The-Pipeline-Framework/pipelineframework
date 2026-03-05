package org.pipelineframework.orchestrator;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
    void enqueueNowErrorMessageIndicatesMissingImplementation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        try {
            dispatcher.enqueueNow(item).await().indefinitely();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("SqsWorkDispatcher"));
            assertTrue(e.getMessage().contains("not implemented"));
        }
    }

    @Test
    void enqueueDelayedErrorMessageIndicatesMissingImplementation() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofSeconds(5);

        try {
            dispatcher.enqueueDelayed(item, delay).await().indefinitely();
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("SqsWorkDispatcher"));
            assertTrue(e.getMessage().contains("not implemented"));
            assertTrue(e.getMessage().contains("deployment-specific"));
        }
    }

    @Test
    void enqueueNowHandlesNullWorkItem() {
        assertThrows(NullPointerException.class,
            () -> dispatcher.enqueueNow(null).await().indefinitely());
    }

    @Test
    void enqueueDelayedHandlesNullWorkItem() {
        Duration delay = Duration.ofSeconds(10);

        assertThrows(NullPointerException.class,
            () -> dispatcher.enqueueDelayed(null, delay).await().indefinitely());
    }

    @Test
    void canInstantiateMultipleDispatchers() {
        SqsWorkDispatcher dispatcher1 = new SqsWorkDispatcher();
        SqsWorkDispatcher dispatcher2 = new SqsWorkDispatcher();

        assertNotNull(dispatcher1);
        assertNotNull(dispatcher2);
        assertNotSame(dispatcher1, dispatcher2);
        assertEquals(dispatcher1.providerName(), dispatcher2.providerName());
        assertEquals(dispatcher1.priority(), dispatcher2.priority());
    }

    @Test
    void enqueueDelayedHandlesZeroDuration() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ZERO;

        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueDelayed(item, delay).await().indefinitely());
    }

    @Test
    void enqueueDelayedHandlesLargeDuration() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofDays(7);

        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueDelayed(item, delay).await().indefinitely());
    }

    @Test
    void enqueueDelayedHandlesNegativeDuration() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofSeconds(-5);

        assertThrows(UnsupportedOperationException.class,
            () -> dispatcher.enqueueDelayed(item, delay).await().indefinitely());
    }
}