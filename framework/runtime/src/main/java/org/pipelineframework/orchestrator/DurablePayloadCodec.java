package org.pipelineframework.orchestrator;

/** Encodes and decodes canonical values at durable boundaries using a pinned release binding. */
public interface DurablePayloadCodec {
    TypedDurablePayload encode(Object value, CanonicalPayloadBinding binding);
    Object decode(TypedDurablePayload payload, CanonicalPayloadBinding binding);

    TypedDurablePayload encode(Object value, CompiledDurablePayloadPlan plan);
    Object decode(TypedDurablePayload payload, CompiledDurablePayloadPlan plan);
}
