package org.pipelineframework.awaitable.kafka;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * SmallRye Reactive Messaging consumer that admits Kafka await completions.
 */
@ApplicationScoped
@IfBuildProperty(name = "pipeline.await.kafka.reactive-messaging.enabled", stringValue = "true")
public class KafkaAwaitCompletionConsumer {

    public static final String INCOMING_CHANNEL = "tpf-await-kafka-responses";

    @Inject
    PipelineExecutionService executionService;

    public KafkaAwaitCompletionConsumer() {
    }

    KafkaAwaitCompletionConsumer(PipelineExecutionService executionService) {
        this.executionService = executionService;
    }

    @Incoming(INCOMING_CHANNEL)
    public CompletionStage<Void> consume(Message<String> message) {
        Objects.requireNonNull(message, "message must not be null");
        return Uni.createFrom().item(() -> parseEnvelope(message.getPayload()))
            .runSubscriptionOn(Infrastructure.getDefaultExecutor())
            .onItem().transformToUni(envelope -> executionService.completeAwaitInteraction(new AwaitCompletionCommand(
                envelope.tenantId(),
                envelope.interactionId(),
                envelope.correlationId(),
                envelope.resumeToken(),
                envelope.idempotencyKey(),
                envelope.responsePayload(),
                envelope.actor(),
                System.currentTimeMillis())))
            .replaceWithVoid()
            .subscribeAsCompletionStage()
            .thenCompose(ignored -> message.ack())
            .exceptionallyCompose(failure -> message.nack(failure));
    }

    private static KafkaAwaitCompletionEnvelope parseEnvelope(String payload) {
        try {
            return PipelineJson.mapper().readValue(payload, KafkaAwaitCompletionEnvelope.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Kafka await completion envelope", e);
        }
    }
}
