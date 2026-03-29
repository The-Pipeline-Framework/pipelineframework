package org.pipelineframework.checkpoint;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Helpers for checkpoint publication metadata and idempotency-key derivation.
 */
public final class CheckpointPublicationSupport {

    private static final Logger LOG = Logger.getLogger(CheckpointPublicationSupport.class);
    private static final JsonFormat.Printer PROTO_JSON = JsonFormat.printer().omittingInsignificantWhitespace();

    /**
     * Prevents instantiation of this utility class.
     */
    private CheckpointPublicationSupport() {
    }

    /**
     * Produce an idempotency key using a provided fallback key or by concatenating configured fields
     * extracted from the payload.
     *
     * @param fallbackKey an optional precomputed key; used (trimmed) when non-null and not blank
     * @param keyFields an ordered list of property names to extract from the payload to form the key
     * @param payload the object from which property values are read
     * @return `fallbackKey.trim()` if `fallbackKey` is non-blank; otherwise the configured payload
     *         property values (trimmed) joined with `":"` when all are present and not blank; `null`
     *         if `payload` or `keyFields` is null/empty or any required field is missing or blank
     */
    public static String deriveIdempotencyKey(String fallbackKey, List<String> keyFields, Object payload) {
        if (fallbackKey != null && !fallbackKey.isBlank()) {
            return fallbackKey.trim();
        }
        if (payload == null || keyFields == null || keyFields.isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>(keyFields.size() + 1);
        for (String field : keyFields) {
            String value = readProperty(payload, field);
            if (value == null || value.isBlank()) {
                return null;
            }
            parts.add(value.trim());
        }
        return String.join(":", parts);
    }

    /**
     * Convert an arbitrary payload to the requested target type, supporting Protobuf messages via JSON conversion.
     *
     * @param payload the source object to normalize; may be a Protobuf MessageOrBuilder or any POJO
     * @param expectedType the target class to convert the payload into
     * @param <T> the target type
     * @return an instance of {@code expectedType} produced from the payload, or {@code null} if {@code payload} or {@code expectedType} is {@code null}
     * @throws IllegalStateException if conversion to the requested type fails
     */
    public static <T> T normalizePayload(Object payload, Class<T> expectedType) {
        if (payload == null || expectedType == null) {
            return null;
        }
        if (expectedType.isInstance(payload)) {
            return expectedType.cast(payload);
        }
        try {
            var json = PipelineJson.mapper();
            var tree = payload instanceof MessageOrBuilder protobuf
                ? json.readTree(PROTO_JSON.print(protobuf))
                : json.valueToTree(payload);
            return json.treeToValue(tree, expectedType);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to normalize checkpoint publication payload to " + expectedType.getName()
                    + " from " + payload.getClass().getName(),
                e);
        }
    }

    /**
     * Extracts a property's string value from a payload object.
     *
     * This method supports Java record components and standard getter naming patterns
     * (property, getProperty, isProperty). If the property exists it is invoked and
     * its toString() value is returned. Reflection failures or missing accessors
     * result in a null return and are logged at trace level.
     *
     * @param payload the object to read the property from
     * @param field the property name to read
     * @return the property's string value, or null if the field is missing, inaccessible, or its value is null
     */
    private static String readProperty(Object payload, String field) {
        if (field == null || field.isBlank()) {
            return null;
        }
        Class<?> type = payload.getClass();
        if (type.isRecord()) {
            for (RecordComponent component : type.getRecordComponents()) {
                if (component.getName().equals(field)) {
                    try {
                        Object value = component.getAccessor().invoke(payload);
                        return value == null ? null : value.toString();
                    } catch (ReflectiveOperationException e) {
                        LOG.tracef(e, "Failed reading checkpoint idempotency field '%s' from record payload %s",
                            field, type.getName());
                        return null;
                    }
                }
            }
        }
        String capitalized = field.substring(0, 1).toUpperCase(Locale.ROOT) + field.substring(1);
        for (String candidate : List.of(field, "get" + capitalized, "is" + capitalized)) {
            try {
                Method method = type.getMethod(candidate);
                Object value = method.invoke(payload);
                return value == null ? null : value.toString();
            } catch (ReflectiveOperationException e) {
                LOG.tracef(e, "Failed reading checkpoint idempotency field '%s' via accessor '%s' on %s",
                    field, candidate, type.getName());
                // Try next accessor name.
            }
        }
        return null;
    }
}
