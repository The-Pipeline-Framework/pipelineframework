/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.connector;

import com.google.protobuf.Message;
import io.smallrye.mutiny.Multi;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.Locale;
import org.jboss.logging.Logger;
import org.pipelineframework.context.TransportDispatchMetadata;

/**
 * Shared connector helpers for backpressure, protobuf field extraction, and deterministic signatures.
 */
public final class ConnectorSupport {
    private static final int DEFAULT_BACKPRESSURE_BUFFER_CAPACITY = 256;
    private static final Logger LOG = Logger.getLogger(ConnectorSupport.class);

    /**
     * Prevents instantiation of this utility class.
     */
    private ConnectorSupport() {
    }

    /**
     * Apply a backpressure strategy to a Reactive Streams Multi source.
     *
     * <p>If the policy is DROP, emitted items are dropped when downstream cannot keep up.
     * If the policy is BUFFER, emitted items are buffered up to the provided capacity; a
     * non-positive capacity selects the default buffer capacity.
     *
     * @param source the source Multi to which backpressure behavior will be applied
     * @param policy the backpressure policy to enforce (DROP or BUFFER)
     * @param backpressureBufferCapacity the buffer size to use when {@code policy} is BUFFER; if
     *        less than or equal to zero the default buffer capacity is used
     * @param <T> the element type of the Multi
     * @return a Multi that enforces the specified backpressure behavior
     */
    public static <T> Multi<T> applyBackpressure(
        Multi<T> source,
        ConnectorBackpressurePolicy policy,
        int backpressureBufferCapacity
    ) {
        return switch (policy) {
            case DROP -> source.onOverflow().drop();
            case BUFFER -> {
                int normalizedCapacity = backpressureBufferCapacity > 0
                    ? backpressureBufferCapacity
                    : DEFAULT_BACKPRESSURE_BUFFER_CAPACITY;
                yield source.onOverflow().buffer(normalizedCapacity);
            }
        };
    }

    /**
     * Convert a textual backpressure policy name to a ConnectorBackpressurePolicy.
     *
     * @param policy the policy name (case-insensitive); recognized value is "DROP"; null or blank is treated as unspecified
     * @return the corresponding ConnectorBackpressurePolicy; `BUFFER` when the input is null, blank, or not recognized
     */
    public static ConnectorBackpressurePolicy normalizeBackpressurePolicy(String policy) {
        if (policy == null || policy.isBlank()) {
            return ConnectorBackpressurePolicy.BUFFER;
        }
        String normalized = policy.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "DROP" -> ConnectorBackpressurePolicy.DROP;
            default -> ConnectorBackpressurePolicy.BUFFER;
        };
    }

    /**
     * Convert a backpressure policy string to the normalized enum name.
     *
     * @param policy the policy name to normalize; null or blank yields "BUFFER", the case-insensitive value "DROP" yields "DROP", any other value yields "BUFFER"
     * @return the name of the corresponding ConnectorBackpressurePolicy enum ("BUFFER" or "DROP")
     */
    public static String normalizeBackpressurePolicyName(String policy) {
        return normalizeBackpressurePolicy(policy).name();
    }

    /**
     * Map a textual idempotency policy name to a {@link ConnectorIdempotencyPolicy} enum.
     *
     * @param policy the policy name (may be null or blank); matching is case-insensitive
     * @return {@code ConnectorIdempotencyPolicy.PRE_FORWARD} for {@code "PRE_FORWARD"}, {@code ConnectorIdempotencyPolicy.ON_ACCEPT} for {@code "ON_ACCEPT"}, or {@code ConnectorIdempotencyPolicy.DISABLED} when the input is null, blank, or unrecognized
     */
    public static ConnectorIdempotencyPolicy normalizeIdempotencyPolicy(String policy) {
        if (policy == null || policy.isBlank()) {
            return ConnectorIdempotencyPolicy.DISABLED;
        }
        String normalized = policy.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "PRE_FORWARD" -> ConnectorIdempotencyPolicy.PRE_FORWARD;
            case "ON_ACCEPT" -> ConnectorIdempotencyPolicy.ON_ACCEPT;
            default -> ConnectorIdempotencyPolicy.DISABLED;
        };
    }

    /**
     * Convert a failure mode name to the corresponding ConnectorFailureMode.
     *
     * @param mode the failure mode name (case-insensitive). Recognized value: "LOG_AND_CONTINUE".
     *             Null or blank selects the default.
     * @return `ConnectorFailureMode.LOG_AND_CONTINUE` if `mode` equals "LOG_AND_CONTINUE" (case-insensitive),
     *         `ConnectorFailureMode.PROPAGATE` otherwise.
     */
    public static ConnectorFailureMode normalizeFailureMode(String mode) {
        if (mode == null || mode.isBlank()) {
            return ConnectorFailureMode.PROPAGATE;
        }
        String normalized = mode.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "LOG_AND_CONTINUE" -> ConnectorFailureMode.LOG_AND_CONTINUE;
            default -> ConnectorFailureMode.PROPAGATE;
        };
    }

    /**
     * Ensures the given dispatch metadata contains an idempotency key by returning metadata that preserves
     * an existing key or populates one derived from the payload when missing.
     *
     * @param existing      the current TransportDispatchMetadata, may be null
     * @param connectorName the connector namespace used when deriving an idempotency key
     * @param payload       the message or object from which idempotency key components are extracted
     * @param keyFields     the list of payload field names used to derive the idempotency key
     * @return the original metadata if it already contains a non-empty idempotency key or if no key can be derived;
     *         otherwise a metadata instance with the derived idempotency key (a new instance is created when
     *         {@code existing} is null) 
     */
    public static TransportDispatchMetadata ensureDispatchMetadata(
        TransportDispatchMetadata existing,
        String connectorName,
        Object payload,
        List<String> keyFields
    ) {
        String existingKey = existing == null ? null : existing.idempotencyKey();
        if (existingKey != null && !existingKey.isBlank()) {
            return existing;
        }
        String derivedKey = deriveIdempotencyKey(connectorName, payload, keyFields);
        if (derivedKey == null || derivedKey.isBlank()) {
            return existing;
        }
        if (existing == null) {
            return new TransportDispatchMetadata(null, null, derivedKey, null, null, null, null);
        }
        return existing.withIdempotencyKey(derivedKey);
    }

    /**
     * Create a deterministic, namespaced idempotency key from the given payload fields.
     *
     * @param connectorName namespace used as the key prefix that identifies the connector
     * @param payload       the object containing fields to derive the key from (may be a protobuf Message, Map, record, or POJO)
     * @param keyFields     ordered list of property names to extract and combine into the key
     * @return              the namespaced deterministic idempotency key, or null if {@code payload} is null or {@code keyFields} is null or empty
     */
    public static String deriveIdempotencyKey(String connectorName, Object payload, List<String> keyFields) {
        if (payload == null || keyFields == null || keyFields.isEmpty()) {
            return null;
        }
        List<String> components = new ArrayList<>();
        for (String field : keyFields) {
            components.add(normalizeOrDefault(readProperty(payload, field), ""));
        }
        return deterministicHandoffKey(connectorName, components.toArray(String[]::new));
    }

    /**
     * Extracts a single primitive or enum field value from a protobuf Message by field name.
     *
     * @param message   the protobuf Message to read from
     * @param fieldName the protobuf field name (as declared in the .proto)
     * @return the string representation of the field's primitive or enum value, or an empty string if the field is missing, repeated, a map field, a bytes/message type, or otherwise unsupported
     */
    public static String readField(Message message, String fieldName) {
        var field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null || field.isRepeated() || field.isMapField()) {
            return "";
        }
        Object value = message.getField(field);
        return switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM -> String.valueOf(value);
            case BYTE_STRING, MESSAGE -> "";
        };
    }

    /**
     * Extracts a named property from a payload that may be a protobuf {@code Message}, a {@code Map}, a Java record, or a POJO.
     *
     * <p>Attempts the appropriate lookup strategy for each payload shape and returns an empty string when the property is missing or cannot be read.</p>
     *
     * @param payload the object to read the property from; may be a protobuf {@code Message}, {@code Map}, a record, or a POJO
     * @param propertyName the name of the property to read
     * @return the property's value as a string, or an empty string if the property is not present or cannot be read
     */
    public static String readProperty(Object payload, String propertyName) {
        if (payload == null || propertyName == null || propertyName.isBlank()) {
            return "";
        }
        if (payload instanceof Message message) {
            String direct = readField(message, propertyName);
            if (!direct.isBlank()) {
                return direct;
            }
            return readField(message, toSnakeCase(propertyName));
        }
        if (payload instanceof Map<?, ?> map) {
            Object value = map.get(propertyName);
            return value == null ? "" : String.valueOf(value);
        }
        String recordValue = readRecordProperty(payload, propertyName);
        if (!recordValue.isBlank()) {
            return recordValue;
        }
        String methodValue = readMethodProperty(payload, propertyName);
        return methodValue == null ? "" : methodValue;
    }

    /**
     * Generates a deterministic, namespaced handoff key from the provided namespace and ordered components.
     *
     * The returned key is of the form "<namespace>:<uuid>" where the UUID is deterministically derived
     * from the framed sequence of the namespace and each component. If `namespace` is null or empty it
     * defaults to "handoff". Any null component is treated as an empty string.
     *
     * @param namespace  the namespace to prefix the key (defaults to "handoff" when null/empty)
     * @param components ordered components used to deterministically derive the UUID; null or missing components are treated as empty strings
     * @return           a namespaced deterministic key in the form "<namespace>:<uuid>"
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
     * Builds a sanitized, structured failure signature string for a connector event.
     *
     * <p>Format: {@code connector=<value>;phase=<value>;reason=<value>;traceId=<value>;itemId=<value>}.</p>
     *
     * <p>Null or blank inputs are replaced with defaults: connector and phase -> "unknown",
     * reason -> "unspecified", traceId and itemId -> "na". Each value is sanitized to escape
     * characters that would interfere with parsing (backslash, semicolon, equals, carriage return,
     * newline).</p>
     *
     * @param connector the connector name or identifier
     * @param phase the phase or stage in which the failure occurred
     * @param reason a short reason or classification for the failure
     * @param traceId an associated trace identifier, if available
     * @param itemId an associated item identifier, if available
     * @return the composed, sanitized failure signature string in the format
     *         {@code connector=<value>;phase=<value>;reason=<value>;traceId=<value>;itemId=<value>}
     */
    public static String failureSignature(
        String connector,
        String phase,
        String reason,
        String traceId,
        String itemId
    ) {
        return "connector=" + sanitizeForSignature(normalizeOrDefault(connector, "unknown"))
            + ";phase=" + sanitizeForSignature(normalizeOrDefault(phase, "unknown"))
            + ";reason=" + sanitizeForSignature(normalizeOrDefault(reason, "unspecified"))
            + ";traceId=" + sanitizeForSignature(normalizeOrDefault(traceId, "na"))
            + ";itemId=" + sanitizeForSignature(normalizeOrDefault(itemId, "na"));
    }

    /**
     * Return the trimmed input string, or the provided fallback when the input is null or empty after trimming.
     *
     * @param value    the input string to normalize
     * @param fallback the value to return when {@code value} is null or empty after trimming
     * @return         the trimmed {@code value} when non-empty, otherwise {@code fallback}
     */
    public static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? fallback : normalized;
    }

    /**
     * Retrieve the named component value from a Java record as a string.
     *
     * @param payload      the record instance to read the component from
     * @param propertyName the name of the record component to retrieve
     * @return the component value as a string, or an empty string if the payload is not a record,
     *         the component is not present, the component value is null, or the accessor cannot be invoked
     */
    private static String readRecordProperty(Object payload, String propertyName) {
        if (!payload.getClass().isRecord()) {
            return "";
        }
        for (RecordComponent component : payload.getClass().getRecordComponents()) {
            if (!component.getName().equals(propertyName)) {
                continue;
            }
            try {
                Object value = component.getAccessor().invoke(payload);
                return value == null ? "" : String.valueOf(value);
            } catch (ReflectiveOperationException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.tracef(e, "Reflection failed reading property '%s' on %s",
                        propertyName, payload.getClass().getName());
                }
                return "";
            }
        }
        return "";
    }

    /**
     * Reads a property's value from a POJO by trying common no-arg accessor method names.
     *
     * @param payload the object to read the property from
     * @param propertyName the base property name to look up (will also try `getX` and `isX` variants)
     * @return the property's string value, or an empty string if the property is missing, inaccessible, or null
     */
    private static String readMethodProperty(Object payload, String propertyName) {
        for (String candidate : List.of(
            propertyName,
            "get" + capitalize(propertyName),
            "is" + capitalize(propertyName))) {
            try {
                Method method = payload.getClass().getMethod(candidate);
                Object value = method.invoke(payload);
                return value == null ? "" : String.valueOf(value);
            } catch (ReflectiveOperationException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.tracef(e, "Reflection failed reading property '%s' on %s via method '%s'",
                        propertyName, payload.getClass().getName(), candidate);
                }
            }
        }
        return "";
    }

    /**
         * Convert the first character of the given string to uppercase.
         *
         * @param value the string whose first character will be uppercased
         * @return the input string with its first character uppercased, or an empty string if {@code value} is null or empty
         */
    private static String capitalize(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    /**
     * Converts a camelCase or PascalCase string to snake_case.
     *
     * @param value the input string in camelCase or PascalCase
     * @return the input converted to snake_case (all lowercase with underscores before former uppercase letters)
     */
    private static String toSnakeCase(String value) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (Character.isUpperCase(current) && i > 0) {
                result.append('_');
            }
            result.append(Character.toLowerCase(current));
        }
        return result.toString();
    }

    /**
     * Appends a framed token to the target in the form "#<length>:<value>" where <length> is the number of characters in `value`.
     *
     * @param target the StringBuilder to append the framed token to
     * @param value  the string value to frame; treated as-is (may be empty)
     */
    private static void appendFramed(StringBuilder target, String value) {
        target.append('#').append(value.length()).append(':').append(value);
    }

    /**
     * Escapes characters that could interfere with signature parsing by prefixing them with a backslash.
     *
     * @param value the input string to sanitize
     * @return the input with backslashes, semicolons, equals signs, carriage returns, and newlines escaped
     */
    private static String sanitizeForSignature(String value) {
        return value
            .replace("\\", "\\\\")
            .replace(";", "\\;")
            .replace("=", "\\=")
            .replace("\r", "\\r")
            .replace("\n", "\\n");
    }
}
