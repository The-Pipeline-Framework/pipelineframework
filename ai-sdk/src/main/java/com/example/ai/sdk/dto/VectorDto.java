package com.example.ai.sdk.dto;

import java.util.List;
import java.util.Objects;

/**
 * DTO for Vector entity.
 */
public record VectorDto(String id, List<Float> values) {
    public VectorDto {
        Objects.requireNonNull(values, "values cannot be null");
        values = List.copyOf(values);
    }
}