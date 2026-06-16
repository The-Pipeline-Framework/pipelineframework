package org.pipelineframework.checkpoint.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.CheckpointPublicationEnvelope;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;
import org.pipelineframework.checkpoint.PublicationEncoding;
import org.pipelineframework.checkpoint.PublicationTargetKind;
import org.pipelineframework.checkpoint.ResolvedCheckpointPublicationTarget;
import org.pipelineframework.config.pipeline.PipelineJson;

class CheckpointKafkaPublicationTargetDispatcherTest {

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

    private record PublishedOrder(String orderId, String customerId) {
    }
}
