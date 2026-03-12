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

    private ConnectorSupport() {
    }

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

    public static String normalizeBackpressurePolicyName(String policy) {
        return normalizeBackpressurePolicy(policy).name();
    }

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

    public static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? fallback : normalized;
    }

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

    private static String capitalize(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

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

    private static void appendFramed(StringBuilder target, String value) {
        target.append('#').append(value.length()).append(':').append(value);
    }

    private static String sanitizeForSignature(String value) {
        return value
            .replace("\\", "\\\\")
            .replace(";", "\\;")
            .replace("=", "\\=")
            .replace("\r", "\\r")
            .replace("\n", "\\n");
    }
}
