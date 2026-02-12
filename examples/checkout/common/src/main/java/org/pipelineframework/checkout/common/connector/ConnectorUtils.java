package org.pipelineframework.checkout.common.connector;

import com.google.protobuf.Message;
import io.smallrye.mutiny.Multi;

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
}
