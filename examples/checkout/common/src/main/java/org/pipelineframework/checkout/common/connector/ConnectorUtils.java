package org.pipelineframework.checkout.common.connector;

import com.google.protobuf.Message;
import io.smallrye.mutiny.Multi;

/**
 * Shared connector helpers for backpressure and protobuf field extraction.
 */
public final class ConnectorUtils {

    private ConnectorUtils() {
    }

    public static <T> Multi<T> applyBackpressure(
        Multi<T> source,
        String backpressureStrategy,
        int backpressureBufferCapacity
    ) {
        return switch (backpressureStrategy) {
            case "DROP" -> source.onOverflow().drop();
            case "BUFFER" -> source.onOverflow().buffer(backpressureBufferCapacity);
            default -> throw new IllegalStateException("Unexpected backpressureStrategy: " + backpressureStrategy);
        };
    }

    public static String normalizeBackpressureStrategy(String strategy) {
        if (strategy == null || strategy.isBlank()) {
            return "BUFFER";
        }
        String normalized = strategy.trim().toUpperCase();
        if (!"BUFFER".equals(normalized) && !"DROP".equals(normalized)) {
            return "BUFFER";
        }
        return normalized;
    }

    public static String readField(Message message, String fieldName) {
        var field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            return "";
        }
        Object value = message.getField(field);
        return switch (field.getJavaType()) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ENUM -> String.valueOf(value);
            case BYTE_STRING, MESSAGE -> "";
        };
    }
}
