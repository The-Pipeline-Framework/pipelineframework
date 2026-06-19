package org.pipelineframework.checkpoint.kafka;

/**
 * Kafka publish request for one checkpoint handoff envelope.
 *
 * @param topic Kafka topic
 * @param key Kafka record key
 * @param body serialized envelope body
 */
public record KafkaCheckpointPublicationRequest(
    String topic,
    String key,
    String body
) {
    public KafkaCheckpointPublicationRequest {
        topic = required(topic, "topic");
        key = normalize(key);
        body = required(body, "body");
    }

    private static String required(String value, String field) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
