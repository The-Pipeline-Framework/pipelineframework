package org.pipelineframework.orchestrator.controlplane;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.pipelineframework.config.pipeline.PipelineJson;

final class ControlPlaneChecks {

    private ControlPlaneChecks() {
    }

    static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    static int requireSegmentStopAfterStart(int startStepIndex, int stopBeforeStepIndex) {
        if (stopBeforeStepIndex < -1) {
            throw new IllegalArgumentException("stopBeforeStepIndex must be -1 or greater than or equal to startStepIndex");
        }
        if (stopBeforeStepIndex >= 0 && stopBeforeStepIndex < startStepIndex) {
            throw new IllegalArgumentException("stopBeforeStepIndex must be -1 or greater than or equal to startStepIndex");
        }
        return stopBeforeStepIndex;
    }

    static <T> List<T> copyList(List<T> value) {
        return value == null ? List.of() : List.copyOf(value);
    }

    static <T> Set<T> copySet(Set<T> value) {
        return value == null ? Set.of() : Set.copyOf(value);
    }

    static <K, V> Map<K, V> copyMap(Map<K, V> value) {
        return value == null ? Map.of() : Map.copyOf(value);
    }

    static Object freezePayload(Object value) {
        if (value == null || immutableScalar(value)) {
            return value;
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> frozen = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                frozen.put(freezePayload(entry.getKey()), freezePayload(entry.getValue()));
            }
            return Collections.unmodifiableMap(frozen);
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> frozen = new ArrayList<>();
            for (Object item : iterable) {
                frozen.add(freezePayload(item));
            }
            return List.copyOf(frozen);
        }
        Class<?> type = value.getClass();
        if (type.isArray()) {
            int length = Array.getLength(value);
            List<Object> frozen = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                frozen.add(freezePayload(Array.get(value, i)));
            }
            return List.copyOf(frozen);
        }
        Object normalized;
        try {
            normalized = PipelineJson.mapper().convertValue(value, Object.class);
        } catch (IllegalArgumentException failure) {
            throw unsupportedPayload(value, failure);
        }
        if (normalized == value) {
            throw unsupportedPayload(value, null);
        }
        return freezePayload(normalized);
    }

    private static IllegalArgumentException unsupportedPayload(Object value, Throwable cause) {
        String message = "Unsupported control-plane payload type: " + value.getClass().getName();
        return cause == null ? new IllegalArgumentException(message) : new IllegalArgumentException(message, cause);
    }

    private static boolean immutableScalar(Object value) {
        return value instanceof String
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Double
            || value instanceof Float
            || value instanceof Short
            || value instanceof Byte
            || value instanceof BigDecimal
            || value instanceof BigInteger
            || value instanceof Boolean
            || value instanceof Character
            || value instanceof Enum<?>
            || value instanceof UUID
            || value instanceof URI
            || value instanceof URL
            || value instanceof Instant
            || value instanceof LocalDate
            || value instanceof LocalDateTime
            || value instanceof OffsetDateTime
            || value instanceof ZonedDateTime;
    }
}
