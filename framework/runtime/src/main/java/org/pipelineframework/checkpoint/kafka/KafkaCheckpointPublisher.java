package org.pipelineframework.checkpoint.kafka;

import io.smallrye.mutiny.Uni;

/**
 * Broker boundary used by the Kafka checkpoint handoff dispatcher.
 */
public interface KafkaCheckpointPublisher {

    /**
     * Publishes one checkpoint handoff envelope.
     *
     * @param request publish request
     * @return completion signal
     */
    Uni<Void> publish(KafkaCheckpointPublicationRequest request);
}
