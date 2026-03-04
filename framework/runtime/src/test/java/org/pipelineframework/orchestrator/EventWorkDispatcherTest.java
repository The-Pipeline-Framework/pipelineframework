package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import jakarta.enterprise.event.Event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EventWorkDispatcherTest {

    private EventWorkDispatcher dispatcher;
    private Event<ExecutionWorkItem> mockEvent;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        dispatcher = new EventWorkDispatcher();
        mockEvent = mock(Event.class);
        // Use reflection to inject the mock event
        try {
            var field = EventWorkDispatcher.class.getDeclaredField("executionWorkEvent");
            field.setAccessible(true);
            field.set(dispatcher, mockEvent);
        } catch (Exception e) {
            fail("Failed to inject mock event: " + e.getMessage());
        }
    }

    @Test
    void providerNameIsEvent() {
        assertEquals("event", dispatcher.providerName());
    }

    @Test
    void priorityIsNegative() {
        assertEquals(-100, dispatcher.priority());
    }

    @Test
    void enqueueNowFiresAsyncEvent() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        dispatcher.enqueueNow(item).await().indefinitely();

        verify(mockEvent).fireAsync(item);
    }

    @Test
    void enqueueDelayedSchedulesEvent() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        when(mockEvent.fireAsync(any())).thenAnswer(invocation -> {
            latch.countDown();
            return null;
        });

        ExecutionWorkItem item = new ExecutionWorkItem("tenant2", "exec2");

        dispatcher.enqueueDelayed(item, Duration.ofMillis(50)).await().indefinitely();

        // Wait for the delayed event to fire
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        verify(mockEvent).fireAsync(item);
    }

    @Test
    void enqueueDelayedHandlesZeroDuration() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant3", "exec3");

        dispatcher.enqueueDelayed(item, Duration.ZERO).await().indefinitely();

        // Should schedule immediately
        verify(mockEvent, timeout(1000)).fireAsync(item);
    }

    @Test
    void enqueueDelayedHandlesNullDuration() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant4", "exec4");

        dispatcher.enqueueDelayed(item, null).await().indefinitely();

        // Should treat as immediate
        verify(mockEvent, timeout(1000)).fireAsync(item);
    }

    @Test
    void shutdownStopsScheduler() {
        // This test verifies shutdown doesn't throw
        assertDoesNotThrow(() -> dispatcher.shutdown());
    }

    @Test
    void enqueueNowReturnsCompletedUni() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant5", "exec5");

        var result = dispatcher.enqueueNow(item);

        assertNotNull(result);
        assertDoesNotThrow(() -> result.await().indefinitely());
    }

    @Test
    void enqueueDelayedReturnsCompletedUni() {
        ExecutionWorkItem item = new ExecutionWorkItem("tenant6", "exec6");

        var result = dispatcher.enqueueDelayed(item, Duration.ofMillis(10));

        assertNotNull(result);
        assertDoesNotThrow(() -> result.await().indefinitely());
    }
}