package org.pipelineframework.checkpoint.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkpoint.CheckpointPublicationEnvelope;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;
import org.pipelineframework.checkpoint.CheckpointPublicationTargetDispatcher;
import org.pipelineframework.checkpoint.PipelineHandoffConfig;
import org.pipelineframework.checkpoint.PublicationEncoding;
import org.pipelineframework.checkpoint.PublicationTargetKind;
import org.pipelineframework.checkpoint.ResolvedCheckpointPublicationTarget;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Kafka dispatcher for runtime checkpoint publication targets.
 */
@ApplicationScoped
@Unremovable
public class KafkaCheckpointPublicationTargetDispatcher implements CheckpointPublicationTargetDispatcher {

    @Inject
    Instance<KafkaCheckpointPublisher> publishers;

    private final KafkaCheckpointPublisher explicitPublisher;

    public KafkaCheckpointPublicationTargetDispatcher() {
        this.explicitPublisher = null;
    }

    KafkaCheckpointPublicationTargetDispatcher(KafkaCheckpointPublisher publisher) {
        this.explicitPublisher = publisher;
    }

    @Override
    public PublicationTargetKind kind() {
        return PublicationTargetKind.KAFKA;
    }

    @Override
    public ResolvedCheckpointPublicationTarget resolveTarget(
        String publication,
        String targetId,
        PipelineHandoffConfig.TargetConfig target
    ) {
        String topic = target.topic()
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "Checkpoint publication '" + publication + "' target '" + targetId
                    + "' requires topic for KAFKA delivery"));
        return new ResolvedCheckpointPublicationTarget(
            publication,
            targetId,
            PublicationTargetKind.KAFKA,
            PublicationEncoding.JSON,
            null,
            null,
            topic,
            "PUBLISH");
    }

    @Override
    public Uni<Void> dispatch(
        ResolvedCheckpointPublicationTarget target,
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    ) {
        String key = idempotencyKey == null || idempotencyKey.isBlank() ? request.publication() : idempotencyKey.trim();
        CheckpointPublicationEnvelope envelope = new CheckpointPublicationEnvelope(
            request.publication(),
            tenantId,
            idempotencyKey,
            request.payload(),
            System.currentTimeMillis());
        String body;
        try {
            body = PipelineJson.mapper().writeValueAsString(envelope);
        } catch (Exception e) {
            return Uni.createFrom().failure(new IllegalStateException("Failed serializing Kafka checkpoint envelope", e));
        }
        return publisher().publish(new KafkaCheckpointPublicationRequest(target.endpoint(), key, body));
    }

    private KafkaCheckpointPublisher publisher() {
        if (explicitPublisher != null) {
            return explicitPublisher;
        }
        return publishers.stream()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "Kafka checkpoint handoff requires a KafkaCheckpointPublisher provider. "
                    + "Enable tpf.checkpoint.kafka.publisher.enabled=true and configure the TPF checkpoint Kafka channel."));
    }
}
