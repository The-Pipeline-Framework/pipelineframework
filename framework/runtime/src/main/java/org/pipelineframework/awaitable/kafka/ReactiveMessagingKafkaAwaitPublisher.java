package org.pipelineframework.awaitable.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * SmallRye Reactive Messaging backed Kafka publisher for await dispatch.
 */
@ApplicationScoped
@IfBuildProperty(name = "pipeline.await.kafka.reactive-messaging.enabled", stringValue = "true")
public class ReactiveMessagingKafkaAwaitPublisher implements KafkaAwaitPublisher {

    public static final String OUTGOING_CHANNEL = "tpf-await-kafka-requests";

    @Inject
    @Channel(OUTGOING_CHANNEL)
    MutinyEmitter<String> emitter;

    @Override
    public Uni<Void> publish(KafkaAwaitPublishRequest request) {
        RecordHeaders kafkaHeaders = new RecordHeaders();
        for (Map.Entry<String, String> entry : request.headers().entrySet()) {
            kafkaHeaders.add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
            .withTopic(request.topic())
            .withKey(request.key())
            .withHeaders(kafkaHeaders)
            .build();
        Message<String> message = Message.of(request.body()).addMetadata(metadata);
        return emitter.sendMessage(message);
    }
}
