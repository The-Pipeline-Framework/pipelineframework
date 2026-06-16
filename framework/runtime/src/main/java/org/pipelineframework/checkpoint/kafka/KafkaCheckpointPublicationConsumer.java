package org.pipelineframework.checkpoint.kafka;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.pipelineframework.checkpoint.CheckpointPublicationAdmissionService;
import org.pipelineframework.checkpoint.CheckpointPublicationEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * SmallRye Reactive Messaging consumer that admits Kafka checkpoint handoffs.
 */
@ApplicationScoped
@IfBuildProperty(name = "tpf.checkpoint.kafka.consumer.enabled", stringValue = "true")
public class KafkaCheckpointPublicationConsumer {

    public static final String INCOMING_CHANNEL = "tpf-checkpoint-kafka-publications";
    private static final Logger LOG = Logger.getLogger(KafkaCheckpointPublicationConsumer.class);

    @Inject
    CheckpointPublicationAdmissionService admissionService;

    public KafkaCheckpointPublicationConsumer() {
    }

    KafkaCheckpointPublicationConsumer(CheckpointPublicationAdmissionService admissionService) {
        this.admissionService = admissionService;
    }

    @Incoming(INCOMING_CHANNEL)
    public CompletionStage<Void> consume(Message<String> message) {
        Objects.requireNonNull(message, "message must not be null");
        return Uni.createFrom().item(() -> parseEnvelope(message.getPayload()))
            .runSubscriptionOn(Infrastructure.getDefaultExecutor())
            .onItem().transformToUni(envelope -> admissionService.admit(
                envelope.toRequest(),
                envelope.tenantId(),
                envelope.idempotencyKey()))
            .replaceWithVoid()
            .subscribeAsCompletionStage()
            .thenCompose(ignored -> message.ack())
            .exceptionallyCompose(failure -> {
                Throwable cause = unwrap(failure);
                if (cause instanceof NotFoundException) {
                    LOG.warnf("Dropping Kafka checkpoint handoff without local subscriber: %s", cause.getMessage());
                    return message.ack();
                }
                return message.nack(cause);
            });
    }

    private static CheckpointPublicationEnvelope parseEnvelope(String payload) {
        try {
            return PipelineJson.mapper().readValue(payload, CheckpointPublicationEnvelope.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Kafka checkpoint publication envelope", e);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
