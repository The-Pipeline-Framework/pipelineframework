package org.pipelineframework.awaitable.kafka;

import io.smallrye.mutiny.Uni;

/**
 * Broker boundary used by the Kafka await transport adapter.
 */
public interface KafkaAwaitPublisher {

    /**
     * Publishes one await request envelope.
     *
     * @param request publish request
     * @return completion signal
     */
    Uni<Void> publish(KafkaAwaitPublishRequest request);
}
