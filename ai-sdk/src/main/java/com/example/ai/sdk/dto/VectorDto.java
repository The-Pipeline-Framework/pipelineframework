package com.example.ai.sdk.dto;

import java.util.List;

/**
 * DTO for Vector entity.
 */
public record VectorDto(String id, List<Float> values) {
}