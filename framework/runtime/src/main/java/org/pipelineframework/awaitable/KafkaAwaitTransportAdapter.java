package org.pipelineframework.awaitable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.awaitable.kafka.KafkaAwaitDispatchEnvelope;
import org.pipelineframework.awaitable.kafka.KafkaAwaitPublishRequest;
import org.pipelineframework.awaitable.kafka.KafkaAwaitPublisher;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Await transport adapter that publishes one interaction request to Kafka.
 */
@ApplicationScoped
public class KafkaAwaitTransportAdapter implements AwaitTransportAdapter<Object> {

    private static final String DEFAULT_KEY_STRATEGY = "interactionId";

    @Inject
    Instance<KafkaAwaitPublisher> publishers;

    @Inject
    AwaitResumeTokenService resumeTokenService;

    private final KafkaAwaitPublisher explicitPublisher;

    public KafkaAwaitTransportAdapter() {
        this(null);
    }

    KafkaAwaitTransportAdapter(KafkaAwaitPublisher explicitPublisher) {
        this.explicitPublisher = explicitPublisher;
    }

    @Override
    public String type() {
        return "kafka";
    }

    @Override
    public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
        Objects.requireNonNull(request, "request must not be null");
        AwaitStepDescriptor descriptor = request.descriptor();
        AwaitInteractionRecord interaction = request.interaction();
        KafkaConfig config = KafkaConfig.from(descriptor.transportConfig());
        String resumeToken = resumeTokenService.sign(interaction, System.currentTimeMillis());
        Map<String, Object> metadata = dispatchMetadata(config, interaction);
        KafkaAwaitDispatchEnvelope envelope = KafkaAwaitDispatchEnvelope.from(
            descriptor,
            interaction,
            request.payload(),
            resumeToken,
            metadata);
        return Uni.createFrom().item(() -> serializeEnvelope(envelope))
            .runSubscriptionOn(Infrastructure.getDefaultExecutor())
            .onItem().transformToUni(body -> {
                KafkaAwaitPublishRequest publishRequest = new KafkaAwaitPublishRequest(
                    config.requestTopic(),
                    key(config.keyStrategy(), interaction),
                    headers(config, interaction),
                    body);
                return publisher().publish(publishRequest)
                    .replaceWith(new AwaitDispatchResult(metadata));
            });
    }

    private KafkaAwaitPublisher publisher() {
        if (explicitPublisher != null) {
            return explicitPublisher;
        }
        if (publishers == null) {
            throw noPublisherFailure();
        }
        List<KafkaAwaitPublisher> candidates = publishers.stream().toList();
        if (candidates.isEmpty()) {
            throw noPublisherFailure();
        }
        if (candidates.size() > 1) {
            String providers = candidates.stream()
                .map(candidate -> candidate.getClass().getName())
                .collect(java.util.stream.Collectors.joining(", "));
            throw new IllegalStateException("Ambiguous KafkaAwaitPublisher providers: " + providers);
        }
        return candidates.getFirst();
    }

    private static IllegalStateException noPublisherFailure() {
        return new IllegalStateException(
            "Kafka await transport requires a KafkaAwaitPublisher provider. "
                + "Enable tpf.await.kafka.reactive-messaging.enabled=true and configure the TPF await Kafka channels, "
                + "or provide a CDI KafkaAwaitPublisher bean.");
    }

    private static Map<String, Object> dispatchMetadata(KafkaConfig config, AwaitInteractionRecord interaction) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("adapter", "kafka");
        metadata.put("requestTopic", config.requestTopic());
        metadata.put("responseTopic", config.responseTopic());
        metadata.put("keyStrategy", config.keyStrategy());
        metadata.put("key", key(config.keyStrategy(), interaction));
        if (config.consumerGroup() != null) {
            metadata.put("consumerGroup", config.consumerGroup());
        }
        metadata.put("dispatchedAtEpochMs", System.currentTimeMillis());
        return metadata;
    }

    private static Map<String, String> headers(KafkaConfig config, AwaitInteractionRecord interaction) {
        Map<String, String> headers = new LinkedHashMap<>(config.headers());
        headers.put("tpf-tenant-id", interaction.tenantId());
        headers.put("tpf-execution-id", interaction.executionId());
        headers.put("tpf-interaction-id", interaction.interactionId());
        headers.put("tpf-correlation-id", interaction.correlationId());
        headers.put("tpf-step-id", interaction.stepId());
        headers.put("tpf-response-topic", config.responseTopic());
        return headers;
    }

    private static String key(String strategy, AwaitInteractionRecord interaction) {
        return switch (strategy) {
            case "interactionId" -> interaction.interactionId();
            case "correlationId" -> interaction.correlationId();
            default -> throw new IllegalArgumentException("Unsupported kafka await request.key: " + strategy);
        };
    }

    private static String serializeEnvelope(KafkaAwaitDispatchEnvelope envelope) {
        try {
            return PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing Kafka await dispatch envelope", e);
        }
    }

    record KafkaConfig(
        String requestTopic,
        String responseTopic,
        String keyStrategy,
        String consumerGroup,
        Map<String, String> headers
    ) {
        static KafkaConfig from(Map<String, Object> transportConfig) {
            if (transportConfig == null) {
                throw new IllegalArgumentException("transportConfig must not be null");
            }
            Map<?, ?> request = requiredMap(transportConfig.get("request"), "kafka await transport requires request.topic");
            Map<?, ?> response = requiredMap(transportConfig.get("response"), "kafka await transport requires response.topic");
            String requestTopic = requiredString(request.get("topic"), "kafka await transport requires request.topic");
            String responseTopic = requiredString(response.get("topic"), "kafka await transport requires response.topic");
            String keyStrategy = optionalString(request.get("key"), DEFAULT_KEY_STRATEGY);
            if (!"interactionId".equals(keyStrategy) && !"correlationId".equals(keyStrategy)) {
                throw new IllegalArgumentException("Unsupported kafka await request.key: " + keyStrategy);
            }
            String consumerGroup = optionalString(transportConfig.get("consumerGroup"), null);
            Object consumerObj = transportConfig.get("consumer");
            if (consumerObj instanceof Map<?, ?> consumerMap) {
                consumerGroup = optionalString(consumerMap.get("group"), consumerGroup);
            }
            return new KafkaConfig(
                requestTopic,
                responseTopic,
                keyStrategy,
                consumerGroup,
                stringMap(transportConfig.get("headers")));
        }

        private static Map<?, ?> requiredMap(Object value, String message) {
            if (value instanceof Map<?, ?> map) {
                return map;
            }
            throw new IllegalArgumentException(message);
        }

        private static String requiredString(Object value, String message) {
            String result = optionalString(value, null);
            if (result == null || result.isBlank()) {
                throw new IllegalArgumentException(message);
            }
            return result;
        }

        private static String optionalString(Object value, String fallback) {
            if (value == null) {
                return fallback;
            }
            String text = value.toString().trim();
            return text.isBlank() ? fallback : text;
        }

        private static Map<String, String> stringMap(Object value) {
            if (!(value instanceof Map<?, ?> source)) {
                return Map.of();
            }
            Map<String, String> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    result.put(entry.getKey().toString(), entry.getValue().toString());
                }
            }
            return Map.copyOf(result);
        }
    }
}
