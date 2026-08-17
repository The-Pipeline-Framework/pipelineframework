package org.pipelineframework.query;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** JSON capture codec with explicit support for generated canonical sealed unions. */
final class QueryCapturePayloadCodec {
    private final ObjectMapper json;

    QueryCapturePayloadCodec(ObjectMapper json) {
        this.json = Objects.requireNonNull(json, "query capture JSON mapper must not be null");
    }

    <O> String encode(O output, Class<O> outputType) {
        Objects.requireNonNull(output, "query capture output must not be null");
        Objects.requireNonNull(outputType, "query capture output type must not be null");
        try {
            if (!outputType.isSealed()) {
                return json.writeValueAsString(output);
            }
            Method discriminator = outputType.getMethod("discriminator");
            String value = (String) discriminator.invoke(output);
            Method payload = output.getClass().getMethod("value");
            ObjectNode envelope = json.createObjectNode();
            envelope.put("discriminator", value);
            envelope.put("case", output.getClass().getSimpleName());
            envelope.set("value", json.valueToTree(payload.invoke(output)));
            return json.writeValueAsString(envelope);
        } catch (Exception failure) {
            throw new IllegalArgumentException(
                "Failed encoding captured query output as " + outputType.getName(), failure);
        }
    }

    <O> O decode(String payload, Class<O> outputType) {
        Objects.requireNonNull(payload, "captured query payload must not be null");
        Objects.requireNonNull(outputType, "query capture output type must not be null");
        try {
            if (!outputType.isSealed()) {
                return json.readValue(payload, outputType);
            }
            JsonNode envelope = json.readTree(payload);
            String caseName = requiredText(envelope, "case");
            String expectedDiscriminator = requiredText(envelope, "discriminator");
            JsonNode value = envelope.get("value");
            if (value == null || value.isNull()) {
                throw new IllegalArgumentException("captured query union payload is missing value");
            }
            Class<?> variant = Arrays.stream(outputType.getPermittedSubclasses())
                .filter(candidate -> candidate.getSimpleName().equals(caseName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "captured query union has unknown case '" + caseName + "'"));
            Constructor<?> constructor = Arrays.stream(variant.getDeclaredConstructors())
                .filter(candidate -> candidate.getParameterCount() == 1)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "captured query union case '" + caseName + "' has no unary constructor"));
            Object decodedPayload = json.treeToValue(value, constructor.getParameterTypes()[0]);
            Object decoded = constructor.newInstance(decodedPayload);
            String actualDiscriminator = (String) outputType.getMethod("discriminator").invoke(decoded);
            if (!expectedDiscriminator.equals(actualDiscriminator)) {
                throw new IllegalArgumentException("captured query union discriminator mismatch for case '" + caseName + "'");
            }
            return outputType.cast(decoded);
        } catch (Exception failure) {
            throw new IllegalArgumentException(
                "Failed decoding captured query output as " + outputType.getName(), failure);
        }
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException("captured query union payload is missing " + field);
        }
        return value.textValue();
    }
}
