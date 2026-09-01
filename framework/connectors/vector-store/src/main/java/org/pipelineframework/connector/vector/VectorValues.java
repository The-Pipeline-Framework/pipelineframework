package org.pipelineframework.connector.vector;

import java.util.List;
import java.util.Objects;

final class VectorValues {
    private VectorValues() {
    }

    static String requireText(String value, String label) {
        value = Objects.requireNonNull(value, label + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        return value;
    }

    static List<Float> requireVector(List<Float> values, String label) {
        Objects.requireNonNull(values, label + " must not be null");
        if (values.isEmpty()) {
            throw new IllegalArgumentException(label + " must not be empty");
        }
        for (Float value : values) {
            if (value == null || !Float.isFinite(value)) {
                throw new IllegalArgumentException(label + " must contain only finite values");
            }
        }
        return List.copyOf(values);
    }
}
