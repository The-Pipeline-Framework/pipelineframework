package org.pipelineframework.checkpoint.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * SmallRye Reactive Messaging backed Kafka publisher for checkpoint handoff.
 */
@ApplicationScoped
@IfBuildProperty(name = "tpf.checkpoint.kafka.publisher.enabled", stringValue = "true")
public class ReactiveMessagingKafkaCheckpointPublisher implements KafkaCheckpointPublisher {

    public static final String OUTGOING_CHANNEL = "tpf-checkpoint-kafka-publications";

    @Inject
    @Channel(OUTGOING_CHANNEL)
    MutinyEmitter<String> emitter;

    @Override
    public Uni<Void> publish(KafkaCheckpointPublicationRequest request) {
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
            .withTopic(request.topic())
            .withKey(request.key())
            .build();
        Message<String> message = Message.of(request.body()).addMetadata(metadata);
        return emitter.sendMessage(message);
    }
}
