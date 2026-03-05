package org.pipelineframework.orchestrator;

import java.time.Duration;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WorkDispatcherTest {

    @Test
    void defaultProviderNameIsEvent() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        assertEquals("event", dispatcher.providerName());
    }

    @Test
    void defaultPriorityIsZero() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        assertEquals(0, dispatcher.priority());
    }

    @Test
    void customProviderNameOverridesDefault() {
        WorkDispatcher dispatcher = new WorkDispatcher() {
            @Override
            public String providerName() {
                return "custom-queue";
            }

            @Override
            public Uni<Void> enqueueNow(ExecutionWorkItem item) {
                return Uni.createFrom().voidItem();
            }

            @Override
            public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
                return Uni.createFrom().voidItem();
            }
        };

        assertEquals("custom-queue", dispatcher.providerName());
    }

    @Test
    void customPriorityOverridesDefault() {
        WorkDispatcher dispatcher = new WorkDispatcher() {
            @Override
            public int priority() {
                return 75;
            }

            @Override
            public Uni<Void> enqueueNow(ExecutionWorkItem item) {
                return Uni.createFrom().voidItem();
            }

            @Override
            public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
                return Uni.createFrom().voidItem();
            }
        };

        assertEquals(75, dispatcher.priority());
    }

    @Test
    void enqueueNowMethodRequiresImplementation() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        Uni<Void> result = dispatcher.enqueueNow(item);
        assertNotNull(result);
        result.await().indefinitely();
    }

    @Test
    void enqueueDelayedMethodRequiresImplementation() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofSeconds(30);

        Uni<Void> result = dispatcher.enqueueDelayed(item, delay);
        assertNotNull(result);
        result.await().indefinitely();
    }

    @Test
    void enqueueDelayedHandlesNullDelayGracefully() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");

        Uni<Void> result = dispatcher.enqueueDelayed(item, null);
        assertNotNull(result);
        result.await().indefinitely();
    }

    @Test
    void enqueueDelayedHandlesZeroDuration() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofMillis(0);

        Uni<Void> result = dispatcher.enqueueDelayed(item, delay);
        assertNotNull(result);
        result.await().indefinitely();
    }

    @Test
    void enqueueDelayedHandlesNegativeDuration() {
        WorkDispatcher dispatcher = new TestWorkDispatcher();
        ExecutionWorkItem item = new ExecutionWorkItem("tenant1", "exec1");
        Duration delay = Duration.ofMillis(-100);

        Uni<Void> result = dispatcher.enqueueDelayed(item, delay);
        assertNotNull(result);
        result.await().indefinitely();
    }

    private static class TestWorkDispatcher implements WorkDispatcher {
        @Override
        public Uni<Void> enqueueNow(ExecutionWorkItem item) {
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
            return Uni.createFrom().voidItem();
        }
    }
}