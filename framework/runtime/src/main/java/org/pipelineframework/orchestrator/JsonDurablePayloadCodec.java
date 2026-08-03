package org.pipelineframework.orchestrator;

import jakarta.enterprise.context.ApplicationScoped;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.ByteArrayOutputStream;
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
                + binding.canonicalTypeId() + "': expected=" + binding.runtimeClass().getName()
                + ", actual=" + (value == null ? "<null>" : value.getClass().getName()));
        }
        try {
            return new TypedDurablePayload(
                binding.canonicalTypeId(),
                binding.typeExpressionFingerprint(),
                binding.catalogFingerprint(),
                ENCODING,
                ENCODING_VERSION,
                encodeValue(value, plan));
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
            return decodeValue(payload.payload(), plan);
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

    private static byte[] encodeValue(Object value, CompiledDurablePayloadPlan plan) throws Exception {
        if (!plan.isUnion()) {
            return plan.writer().writeValueAsBytes(value);
        }
        CompiledDurablePayloadPlan.UnionCasePlan unionCase = plan.unionCases().get(value.getClass().getSimpleName());
        if (unionCase == null || !unionCase.runtimeClass().isInstance(value)) {
            throw new IllegalArgumentException("Canonical union " + plan.binding().canonicalTypeId()
                + " has no declared case for " + value.getClass().getName());
        }
        String discriminator;
        try {
            discriminator = (String) plan.discriminator().orElseThrow().invoke(value);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Failed resolving canonical union discriminator for "
                + plan.binding().canonicalTypeId(), e);
        }
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             JsonGenerator generator = PipelineJson.mapper().getFactory().createGenerator(output)) {
            generator.writeStartObject();
            generator.writeStringField("discriminator", discriminator);
            generator.writeStringField("case", value.getClass().getSimpleName());
            generator.writeFieldName("value");
            unionCase.writer().writeValue(generator, value);
            generator.writeEndObject();
            generator.flush();
            return output.toByteArray();
        }
    }

    private static Object decodeValue(byte[] bytes, CompiledDurablePayloadPlan plan) throws Exception {
        if (!plan.isUnion()) {
            return plan.reader().readValue(bytes);
        }
        JsonNode envelope = PipelineJson.mapper().readTree(bytes);
        JsonNode caseNode = envelope.get("case");
        JsonNode valueNode = envelope.get("value");
        if (caseNode == null || !caseNode.isTextual() || valueNode == null) {
            throw new IllegalArgumentException("Malformed canonical union durable payload '"
                + plan.binding().canonicalTypeId() + "'");
        }
        CompiledDurablePayloadPlan.UnionCasePlan unionCase = plan.unionCases().get(caseNode.textValue());
        if (unionCase == null) {
            throw new IllegalArgumentException("Unknown canonical union case '" + caseNode.textValue()
                + "' for " + plan.binding().canonicalTypeId());
        }
        Object value = unionCase.reader().readValue(valueNode.traverse(PipelineJson.mapper()));
        String expectedDiscriminator = envelope.path("discriminator").asText();
        String actualDiscriminator;
        try {
            actualDiscriminator = (String) plan.discriminator().orElseThrow().invoke(value);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Failed resolving canonical union discriminator for "
                + plan.binding().canonicalTypeId(), e);
        }
        if (!expectedDiscriminator.equals(actualDiscriminator)) {
            throw new IllegalArgumentException("Canonical union discriminator mismatch for "
                + plan.binding().canonicalTypeId());
        }
        return value;
    }
}
