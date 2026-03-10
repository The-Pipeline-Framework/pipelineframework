package org.pipelineframework.reject;

import java.util.List;
import java.util.Map;

import io.quarkus.runtime.LaunchMode;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.context.TransportDispatchMetadata;
import org.pipelineframework.context.TransportDispatchMetadataHolder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ItemRejectRouterTest {

    @AfterEach
    void clearThreadLocals() {
        PipelineContextHolder.clear();
        TransportDispatchMetadataHolder.clear();
    }

    @Test
    void productionStartupFailsWhenRecoverOnFailureEnabledAndSinkIsNonDurable() {
        String previousProfile = System.getProperty("quarkus.profile");
        try {
            System.setProperty("quarkus.profile", "prod");

            ItemRejectConfig config = mockBaseConfig(false, ItemRejectFailurePolicy.CONTINUE, true, "log");
            PipelineStepConfig stepConfig = mockStepConfig(true);
            ItemRejectSink logSink = new CapturingSink("log", -100, false, false);

            ItemRejectRouter router = new ItemRejectRouter(
                config,
                stepConfig,
                mockSinks(logSink),
                LaunchMode.NORMAL);

            IllegalStateException error = assertThrows(IllegalStateException.class, router::initialize);
            assertTrue(error.getMessage().contains("non-durable"));
        } finally {
            if (previousProfile == null) {
                System.clearProperty("quarkus.profile");
            } else {
                System.setProperty("quarkus.profile", previousProfile);
            }
        }
    }

    @Test
    void nonProductionStartupAllowsNonDurableSinkWithRecoverOnFailureEnabled() {
        ItemRejectConfig config = mockBaseConfig(false, ItemRejectFailurePolicy.CONTINUE, true, "log");
        PipelineStepConfig stepConfig = mockStepConfig(true);
        ItemRejectSink logSink = new CapturingSink("log", -100, false, false);

        ItemRejectRouter router = new ItemRejectRouter(
            config,
            stepConfig,
            mockSinks(logSink),
            LaunchMode.TEST);

        assertDoesNotThrow(router::initialize);
    }

    @Test
    void publishItemRejectOmitsPayloadWhenDisabled() {
        ItemRejectConfig config = mockBaseConfig(false, ItemRejectFailurePolicy.CONTINUE, true, "capture");
        PipelineStepConfig stepConfig = mockStepConfig(false);
        CapturingSink sink = new CapturingSink("capture", -100, true, false);

        ItemRejectRouter router = new ItemRejectRouter(
            config,
            stepConfig,
            mockSinks(sink),
            LaunchMode.NORMAL);
        router.initialize();

        TransportDispatchMetadataHolder.set(new TransportDispatchMetadata(
            "corr-1", "exec-1", "idem-1", 2, null, null, null));
        PipelineContextHolder.set(new PipelineContext("v1", "none", "default"));

        router.publishItemReject(String.class, Map.of("id", 7), new RuntimeException("boom"), 2, 3)
            .await().indefinitely();

        ItemRejectEnvelope envelope = sink.lastEnvelope;
        assertNotNull(envelope);
        assertNull(envelope.payload());
        assertEquals("exec-1", envelope.executionId());
        assertEquals("corr-1", envelope.correlationId());
        assertEquals("none", envelope.replayMode());
        assertEquals(3, envelope.finalAttempt());
        assertNotNull(envelope.itemFingerprint());
    }

    @Test
    void publishItemRejectIncludesPayloadWhenEnabled() {
        ItemRejectConfig config = mockBaseConfig(true, ItemRejectFailurePolicy.CONTINUE, true, "capture");
        PipelineStepConfig stepConfig = mockStepConfig(false);
        CapturingSink sink = new CapturingSink("capture", -100, true, false);

        ItemRejectRouter router = new ItemRejectRouter(
            config,
            stepConfig,
            mockSinks(sink),
            LaunchMode.NORMAL);
        router.initialize();

        router.publishItemReject(String.class, List.of("a", "b"), new RuntimeException("boom"), 0, 3)
            .await().indefinitely();

        ItemRejectEnvelope envelope = sink.lastEnvelope;
        assertNotNull(envelope);
        assertEquals(List.of("a", "b"), envelope.payload());
        assertEquals("ITEM", envelope.rejectScope());
    }

    @Test
    void continuePolicySwallowsPublishFailures() {
        ItemRejectConfig config = mockBaseConfig(false, ItemRejectFailurePolicy.CONTINUE, true, "capture");
        PipelineStepConfig stepConfig = mockStepConfig(false);
        CapturingSink sink = new CapturingSink("capture", -100, true, true);

        ItemRejectRouter router = new ItemRejectRouter(
            config,
            stepConfig,
            mockSinks(sink),
            LaunchMode.NORMAL);
        router.initialize();

        assertDoesNotThrow(() ->
            router.publishItemReject(String.class, "x", new RuntimeException("boom"), 0, 1)
                .await().indefinitely());
    }

    @Test
    void failPipelinePolicyPropagatesPublishFailures() {
        ItemRejectConfig config = mockBaseConfig(false, ItemRejectFailurePolicy.FAIL_PIPELINE, true, "capture");
        PipelineStepConfig stepConfig = mockStepConfig(false);
        CapturingSink sink = new CapturingSink("capture", -100, true, true);

        ItemRejectRouter router = new ItemRejectRouter(
            config,
            stepConfig,
            mockSinks(sink),
            LaunchMode.NORMAL);
        router.initialize();

        assertThrows(RuntimeException.class, () ->
            router.publishItemReject(String.class, "x", new RuntimeException("boom"), 0, 1)
                .await().indefinitely());
    }

    @Test
    void higherPriorityProviderIsSelected() {
        ItemRejectConfig config = mockBaseConfig(false, ItemRejectFailurePolicy.CONTINUE, true, "capture");
        PipelineStepConfig stepConfig = mockStepConfig(false);
        CapturingSink lowPriority = new CapturingSink("capture", -200, true, false);
        CapturingSink highPriority = new CapturingSink("capture", -100, true, false);

        ItemRejectRouter router = new ItemRejectRouter(
            config,
            stepConfig,
            mockSinks(lowPriority, highPriority),
            LaunchMode.NORMAL);
        router.initialize();

        router.publishItemReject(String.class, "x", new RuntimeException("boom"), 0, 1)
            .await().indefinitely();

        assertNotNull(highPriority.lastEnvelope);
        assertNull(lowPriority.lastEnvelope);
    }

    @SuppressWarnings("unchecked")
    private static Instance<ItemRejectSink> mockSinks(ItemRejectSink... sinks) {
        Instance<ItemRejectSink> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(java.util.Arrays.stream(sinks));
        return instance;
    }

    private static ItemRejectConfig mockBaseConfig(
        boolean includePayload,
        ItemRejectFailurePolicy failurePolicy,
        boolean strictStartup,
        String provider
    ) {
        ItemRejectConfig config = mock(ItemRejectConfig.class);
        ItemRejectConfig.SqsConfig sqsConfig = mock(ItemRejectConfig.SqsConfig.class);
        when(config.provider()).thenReturn(provider);
        when(config.includePayload()).thenReturn(includePayload);
        when(config.publishFailurePolicy()).thenReturn(failurePolicy);
        when(config.strictStartup()).thenReturn(strictStartup);
        when(config.memoryCapacity()).thenReturn(16);
        when(config.sqs()).thenReturn(sqsConfig);
        when(sqsConfig.queueUrl()).thenReturn(java.util.Optional.of("https://sqs.local/queue/reject"));
        when(sqsConfig.region()).thenReturn(java.util.Optional.empty());
        when(sqsConfig.endpointOverride()).thenReturn(java.util.Optional.empty());
        return config;
    }

    private static PipelineStepConfig mockStepConfig(boolean recoverOnFailure) {
        PipelineStepConfig config = mock(PipelineStepConfig.class);
        PipelineStepConfig.StepConfig defaults = mock(PipelineStepConfig.StepConfig.class);
        when(config.defaults()).thenReturn(defaults);
        when(defaults.recoverOnFailure()).thenReturn(recoverOnFailure);
        when(config.step()).thenReturn(Map.of());
        return config;
    }

    private static final class CapturingSink implements ItemRejectSink {
        private final String provider;
        private final int priority;
        private final boolean durable;
        private final boolean failPublish;
        private ItemRejectEnvelope lastEnvelope;

        private CapturingSink(String provider, int priority, boolean durable, boolean failPublish) {
            this.provider = provider;
            this.priority = priority;
            this.durable = durable;
            this.failPublish = failPublish;
        }

        @Override
        public String providerName() {
            return provider;
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public boolean durable() {
            return durable;
        }

        @Override
        public Uni<Void> publish(ItemRejectEnvelope envelope) {
            this.lastEnvelope = envelope;
            if (failPublish) {
                return Uni.createFrom().failure(new RuntimeException("publish failed"));
            }
            return Uni.createFrom().voidItem();
        }
    }
}
