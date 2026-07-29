package org.pipelineframework.orchestrator;

import jakarta.enterprise.context.ApplicationScoped;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.pipelineframework.config.pipeline.PipelineJson;

/** JSON implementation of the release-bound durable canonical payload contract. */
@ApplicationScoped
public class JsonDurablePayloadCodec implements DurablePayloadCodec {
    public static final String ENCODING = "application/tpf-canonical+json";
    public static final int ENCODING_VERSION = 1;

    @Override
    public TypedDurablePayload encode(Object value, CanonicalPayloadBinding binding) {
        return encode(value, CompiledDurablePayloadPlan.compile(binding));
    }

    @Override
    public TypedDurablePayload encode(Object value, CompiledDurablePayloadPlan plan) {
        CanonicalPayloadBinding binding = plan.binding();
        if (value == null || !binding.runtimeClass().isInstance(value)) {
            throw new IllegalArgumentException("Durable payload value is incompatible with canonical type '"
                + binding.canonicalTypeId() + "'");
        }
        try {
            return new TypedDurablePayload(
                binding.canonicalTypeId(),
                binding.typeExpressionFingerprint(),
                binding.catalogFingerprint(),
                ENCODING,
                ENCODING_VERSION,
                plan.writer().writeValueAsBytes(value));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed encoding canonical durable payload '"
                + binding.canonicalTypeId() + "'", e);
        }
    }

    @Override
    public Object decode(TypedDurablePayload payload, CanonicalPayloadBinding binding) {
        return decode(payload, CompiledDurablePayloadPlan.compile(binding));
    }

    @Override
    public Object decode(TypedDurablePayload payload, CompiledDurablePayloadPlan plan) {
        CanonicalPayloadBinding binding = plan.binding();
        validate(payload, binding);
        try {
            return plan.reader().readValue(payload.payload());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed decoding canonical durable payload '"
                + binding.canonicalTypeId() + "'", e);
        }
    }

    private static void validate(TypedDurablePayload payload, CanonicalPayloadBinding binding) {
        if (!ENCODING.equals(payload.encoding()) || payload.encodingVersion() != ENCODING_VERSION) {
            throw new IllegalArgumentException("Unsupported canonical durable payload encoding '" + payload.encoding() + "'");
        }
        if (!binding.canonicalTypeId().equals(payload.canonicalTypeId())
            || !binding.typeExpressionFingerprint().equals(payload.typeExpressionFingerprint())
            || !binding.catalogFingerprint().equals(payload.catalogFingerprint())) {
            throw new IllegalArgumentException("Durable payload binding does not match canonical release contract for '"
                + binding.canonicalTypeId() + "'");
        }
    }
}
