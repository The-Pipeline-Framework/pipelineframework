package org.pipelineframework.orchestrator;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterPublisherTest {

    @Test
    void defaultProviderNameIsLog() {
        DeadLetterPublisher publisher = new TestPublisher();
        assertEquals("log", publisher.providerName());
    }

    @Test
    void defaultPriorityIsZero() {
        DeadLetterPublisher publisher = new TestPublisher();
        assertEquals(0, publisher.priority());
    }

    @Test
    void loggingPublisherExposesExpectedProviderContract() {
        LoggingDeadLetterPublisher publisher = new LoggingDeadLetterPublisher();
        assertEquals("log", publisher.providerName());
        assertEquals(-100, publisher.priority());
    }

    @Test
    void canOverrideProviderName() {
        DeadLetterPublisher publisher = new DeadLetterPublisher() {
            @Override
            public String providerName() {
                return "custom-dlq";
            }

            @Override
            public Uni<Void> publish(DeadLetterEnvelope envelope) {
                return Uni.createFrom().voidItem();
            }
        };

        assertEquals("custom-dlq", publisher.providerName());
    }

    @Test
    void canOverridePriority() {
        DeadLetterPublisher publisher = new DeadLetterPublisher() {
            @Override
            public int priority() {
                return 100;
            }

            @Override
            public Uni<Void> publish(DeadLetterEnvelope envelope) {
                return Uni.createFrom().voidItem();
            }
        };

        assertEquals(100, publisher.priority());
    }

    @Test
    void publishMethodMustBeImplemented() {
        DeadLetterPublisher publisher = new TestPublisher();
        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
            "tenant1",
            "exec1",
            "tenant1:exec1:key",
            "corr-1",
            "key1",
            "tpf.orchestrator.execution",
            "OrchestratorService/Run",
            "REST",
            "FUNCTION",
            "FAILED",
            "retry_exhausted",
            "TestError",
            "test message",
            true,
            2,
            System.currentTimeMillis());

        Uni<Void> result = publisher.publish(envelope);
        assertDoesNotThrow(() -> result.await().indefinitely());
    }

    private static class TestPublisher implements DeadLetterPublisher {
        @Override
        public Uni<Void> publish(DeadLetterEnvelope envelope) {
            return Uni.createFrom().voidItem();
        }
    }
}
