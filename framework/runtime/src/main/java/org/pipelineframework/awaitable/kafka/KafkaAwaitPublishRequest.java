package org.pipelineframework.awaitable.kafka;

import java.util.Map;

/**
 * Request sent by the await Kafka adapter to the configured broker client.
 */
public record KafkaAwaitPublishRequest(
    String topic,
    String key,
    Map<String, String> headers,
    String body
) {
    public KafkaAwaitPublishRequest {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
