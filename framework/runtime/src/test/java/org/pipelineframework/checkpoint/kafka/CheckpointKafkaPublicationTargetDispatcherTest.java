package org.pipelineframework.checkpoint.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.CheckpointPublicationEnvelope;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;
import org.pipelineframework.checkpoint.PipelineHandoffConfig;
import org.pipelineframework.checkpoint.PublicationEncoding;
import org.pipelineframework.checkpoint.PublicationTargetKind;
import org.pipelineframework.checkpoint.ResolvedCheckpointPublicationTarget;
import org.pipelineframework.config.pipeline.PipelineJson;

class CheckpointKafkaPublicationTargetDispatcherTest {

    @Test
    void resolveTargetBuildskafkaTargetWithTopic() {
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> Uni.createFrom().voidItem());
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.topic()).thenReturn(Optional.of("checkout.orders.ready.v1"));

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("orders-ready", target.publication());
        assertEquals("deliver", target.targetId());
        assertEquals(PublicationTargetKind.KAFKA, target.kind());
        assertEquals(PublicationEncoding.JSON, target.encoding());
        assertEquals("checkout.orders.ready.v1", target.endpoint());
        assertEquals("PUBLISH", target.method());
    }

    @Test
    void resolveTargetFailsWhenTopicAbsent() {
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> Uni.createFrom().voidItem());
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.topic()).thenReturn(Optional.empty());

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
        assertEquals(
            "Checkpoint publication 'orders-ready' target 'deliver' requires topic for KAFKA delivery",
            error.getMessage());
    }

    @Test
    void resolveTargetFailsWhenTopicBlank() {
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> Uni.createFrom().voidItem());
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.topic()).thenReturn(Optional.of("   "));

        assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
    }

    @Test
    void resolveTargetKindIsKafka() {
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> Uni.createFrom().voidItem());
        assertEquals(PublicationTargetKind.KAFKA, dispatcher.kind());
    }

    @Test
    void dispatchUsesBlankIdempotencyKeyAsPublicationKey() {
        AtomicReference<KafkaCheckpointPublicationRequest> captured = new AtomicReference<>();
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> {
                captured.set(request);
                return Uni.createFrom().voidItem();
            });
        ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
            "orders-ready",
            "deliver",
            PublicationTargetKind.KAFKA,
            PublicationEncoding.JSON,
            null,
            null,
            "checkout.orders.ready.v1",
            "PUBLISH");

        dispatcher.dispatch(
            target,
            new CheckpointPublicationRequest(
                "orders-ready",
                PipelineJson.mapper().valueToTree(new PublishedOrder("o-1", "c-1"))),
            "tenant-1",
            "   ").await().indefinitely();

        assertEquals("orders-ready", captured.get().key());
    }

    @Test
    void dispatchPublishesStrictEnvelopeToConfiguredTopic() throws Exception {
        AtomicReference<KafkaCheckpointPublicationRequest> captured = new AtomicReference<>();
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> {
                captured.set(request);
                return Uni.createFrom().voidItem();
            });
        ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
            "orders-ready",
            "deliver",
            PublicationTargetKind.KAFKA,
            PublicationEncoding.JSON,
            null,
            null,
            "checkout.orders.ready.v1",
            "PUBLISH");

        dispatcher.dispatch(
            target,
            new CheckpointPublicationRequest(
                "orders-ready",
                PipelineJson.mapper().valueToTree(new PublishedOrder("o-1", "c-1"))),
            "tenant-1",
            "idem-1").await().indefinitely();

        KafkaCheckpointPublicationRequest request = captured.get();
        assertEquals("checkout.orders.ready.v1", request.topic());
        assertEquals("idem-1", request.key());
        CheckpointPublicationEnvelope envelope = PipelineJson.mapper()
            .readValue(request.body(), CheckpointPublicationEnvelope.class);
        assertEquals("orders-ready", envelope.publication());
        assertEquals("tenant-1", envelope.tenantId());
        assertEquals("idem-1", envelope.idempotencyKey());
        assertEquals("o-1", envelope.payload().get("orderId").asText());
    }

    @Test
    void dispatchUsesPublicationAsKafkaKeyWhenIdempotencyKeyIsAbsent() {
        AtomicReference<KafkaCheckpointPublicationRequest> captured = new AtomicReference<>();
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher(
            request -> {
                captured.set(request);
                return Uni.createFrom().voidItem();
            });
        ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
            "orders-ready",
            "deliver",
            PublicationTargetKind.KAFKA,
            PublicationEncoding.JSON,
            null,
            null,
            "checkout.orders.ready.v1",
            "PUBLISH");

        dispatcher.dispatch(
            target,
            new CheckpointPublicationRequest(
                "orders-ready",
                PipelineJson.mapper().valueToTree(new PublishedOrder("o-1", "c-1"))),
            "tenant-1",
            null).await().indefinitely();

        assertEquals("orders-ready", captured.get().key());
    }

    @Test
    void dispatchFailsWhenMultiplePublishersAreConfigured() {
        KafkaCheckpointPublicationTargetDispatcher dispatcher = new KafkaCheckpointPublicationTargetDispatcher();
        dispatcher.publishers = publishers(
            request -> Uni.createFrom().voidItem(),
            request -> Uni.createFrom().voidItem());

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.dispatch(
                target(),
                new CheckpointPublicationRequest(
                    "orders-ready",
                    PipelineJson.mapper().valueToTree(new PublishedOrder("o-1", "c-1"))),
                "tenant-1",
                "idem-1"));

        assertEquals(
            "Ambiguous KafkaCheckpointPublisher providers configured for Kafka checkpoint handoff: 2",
            error.getMessage());
    }

    @SuppressWarnings("unchecked")
    private Instance<KafkaCheckpointPublisher> publishers(KafkaCheckpointPublisher... publishers) {
        Instance<KafkaCheckpointPublisher> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(publishers));
        return instance;
    }

    private ResolvedCheckpointPublicationTarget target() {
        return new ResolvedCheckpointPublicationTarget(
            "orders-ready",
            "deliver",
            PublicationTargetKind.KAFKA,
            PublicationEncoding.JSON,
            null,
            null,
            "checkout.orders.ready.v1",
            "PUBLISH");
    }

    private record PublishedOrder(String orderId, String customerId) {
    }
}
