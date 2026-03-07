package org.pipelineframework.checkout.common.connector;

import com.google.protobuf.Message;
import io.smallrye.mutiny.Multi;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Shared connector helpers for backpressure and protobuf field extraction.
 */
public final class ConnectorUtils {
    public static final String BACKPRESSURE_DROP = "DROP";
    public static final String BACKPRESSURE_BUFFER = "BUFFER";
    private static final int DEFAULT_BACKPRESSURE_BUFFER_CAPACITY = 256;

    private ConnectorUtils() {
    }

    public static <T> Multi<T> applyBackpressure(
        Multi<T> source,
        String backpressureStrategy,
        int backpressureBufferCapacity
    ) {
        return switch (backpressureStrategy) {
            case BACKPRESSURE_DROP -> source.onOverflow().drop();
            case BACKPRESSURE_BUFFER -> {
                int normalizedCapacity = backpressureBufferCapacity > 0
                    ? backpressureBufferCapacity
                    : DEFAULT_BACKPRESSURE_BUFFER_CAPACITY;
                yield source.onOverflow().buffer(normalizedCapacity);
            }
            default -> throw new IllegalStateException("Unexpected backpressureStrategy: " + backpressureStrategy);
        };
    }

    public static String normalizeBackpressureStrategy(String strategy) {
        if (strategy == null || strategy.isBlank()) {
            return BACKPRESSURE_BUFFER;
        }
        String normalized = strategy.trim().toUpperCase();
        if (!BACKPRESSURE_BUFFER.equals(normalized) && !BACKPRESSURE_DROP.equals(normalized)) {
            return BACKPRESSURE_BUFFER;
        }
        return normalized;
    }

    /**
     * Read a non-repeated, non-map scalar field from a protobuf Message and return its string representation.
     *
     * @param message   the protobuf Message to read the field from
     * @param fieldName the field name as defined in the message descriptor
     * @return the field's value converted to a String; returns an empty string if the field is not found,
     *         is repeated or a map, or if the field's type is BYTE_STRING or MESSAGE
     */
    public static String readField(Message message, String fieldName) {
        var field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            return "";
        }
        if (field.isRepeated() || field.isMapField()) {
            return "";
        }
        Object value = message.getField(field);
        return switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM -> String.valueOf(value);
            case BYTE_STRING, MESSAGE -> "";
        };
    }

    /**
     * Generate a deterministic handoff key for a namespace and optional components.
     *
     * The result is the normalized namespace, a colon, and a UUID deterministically
     * derived from the namespace and the provided components.
     *
     * @param namespace  the namespace to use as the key prefix; if null or blank, "handoff" is used
     * @param components optional components that contribute to the UUID seed; each null or blank component is treated as an empty string
     * @return a string in the form "<namespace>:<uuid>" where the UUID is name-based and derived from the normalized namespace and components
     */
    public static String deterministicHandoffKey(String namespace, String... components) {
        StringBuilder seed = new StringBuilder();
        appendFramed(seed, normalizeOrDefault(namespace, "handoff"));
        if (components != null) {
            for (String component : components) {
                appendFramed(seed, normalizeOrDefault(component, ""));
            }
        }
        UUID id = UUID.nameUUIDFromBytes(seed.toString().getBytes(StandardCharsets.UTF_8));
        return normalizeOrDefault(namespace, "handoff") + ":" + id;
    }

    /**
     * Constructs a standardized semicolon-delimited failure signature from connector context fields.
     *
     * Null or empty inputs are replaced with sensible defaults before formatting.
     *
     * @param connector name of the connector; defaults to "unknown" when null or empty
     * @param phase processing phase where the failure occurred; defaults to "unknown" when null or empty
     * @param reason human-readable reason for the failure; defaults to "unspecified" when null or empty
     * @param traceId tracing identifier for correlating logs/requests; defaults to "na" when null or empty
     * @param itemId identifier of the affected item; defaults to "na" when null or empty
     * @return a string in the form "connector=...;phase=...;reason=...;traceId=...;itemId=..." with defaults applied
     */
    public static String failureSignature(
        String connector,
        String phase,
        String reason,
        String traceId,
        String itemId
    ) {
        return "connector=" + normalizeOrDefault(connector, "unknown")
            + ";phase=" + normalizeOrDefault(phase, "unknown")
            + ";reason=" + normalizeOrDefault(reason, "unspecified")
            + ";traceId=" + normalizeOrDefault(traceId, "na")
            + ";itemId=" + normalizeOrDefault(itemId, "na");
    }

    /**
     * Return the trimmed input string, or the provided fallback when the input is null or empty after trimming.
     *
     * @param value    the string to normalize; may be null
     * @param fallback the value to return if {@code value} is null or contains only whitespace after trimming
     * @return the trimmed {@code value} when it contains non-whitespace characters, otherwise {@code fallback}
     */
    public static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? fallback : normalized;
    }

    /**
     * Appends a framed representation of a string to the given StringBuilder using the format
     * "#<length>:<value>".
     *
     * @param target the StringBuilder to append into
     * @param value  the string whose framed form will be appended
     */
    private static void appendFramed(StringBuilder target, String value) {
        target.append('#').append(value.length()).append(':').append(value);
    }
}
