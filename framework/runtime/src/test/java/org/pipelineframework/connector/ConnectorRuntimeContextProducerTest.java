package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorRuntimeContextProducerTest {
    @Test
    void selectsZeroOrOneHostAdapter() {
        TestConnectionResolver resolver = new TestConnectionResolver();

        assertTrue(ConnectorRuntimeContextProducer.exactlyOne(List.<ConnectionResolver>of(),
            "ConnectionResolver").isEmpty());
        assertEquals(resolver, ConnectorRuntimeContextProducer.exactlyOne(List.of(resolver),
            "ConnectionResolver").orElseThrow());
    }

    @Test
    void rejectsAmbiguousHostAdaptersDeterministically() {
        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
            ConnectorRuntimeContextProducer.exactlyOne(
                List.of(new TestConnectionResolver(), new AnotherConnectionResolver()), "ConnectionResolver"));

        assertTrue(failure.getMessage().contains("Multiple ConnectionResolver beans are registered"));
        assertTrue(failure.getMessage().contains(AnotherConnectionResolver.class.getName()));
        assertTrue(failure.getMessage().contains(TestConnectionResolver.class.getName()));
    }

    private static class TestConnectionResolver implements ConnectionResolver {
        @Override
        public <C extends ResolvedConnection> java.util.concurrent.CompletionStage<C> resolve(
            ConnectionResolutionRequest<C> request
        ) {
            return java.util.concurrent.CompletableFuture.failedStage(new UnsupportedOperationException());
        }
    }

    private static final class AnotherConnectionResolver extends TestConnectionResolver {
    }
}
