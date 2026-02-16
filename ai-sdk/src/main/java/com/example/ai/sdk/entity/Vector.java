package com.example.ai.sdk.entity;

import java.util.List;
import java.util.Objects;

/**
 * Represents an embedding vector.
 */
public record Vector(String id, List<Float> values) {
    public Vector {
        Objects.requireNonNull(values, "values cannot be null");
        values = List.copyOf(values);
    }
}