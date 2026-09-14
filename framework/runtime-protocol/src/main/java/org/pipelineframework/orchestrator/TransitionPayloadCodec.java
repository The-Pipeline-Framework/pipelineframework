package org.pipelineframework.orchestrator;

/**
 * Encodes and decodes payloads that cross the transition-worker seam.
 */
public interface TransitionPayloadCodec {

    /**
     * Encoding identifier placed on serialized transition payloads.
     *
     * @return encoding identifier
     */
    String encoding();

    /**
     * Encodes a payload for a transition envelope.
     *
     * @param payload payload to encode
     * @return serialized payload
     */
    SerializedTransitionPayload encode(Object payload);

    /**
     * Decodes a payload from a transition envelope.
     *
     * @param payload serialized payload
     * @return decoded payload
     */
    Object decode(SerializedTransitionPayload payload);
}
