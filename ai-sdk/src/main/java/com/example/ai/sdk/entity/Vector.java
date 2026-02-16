package com.example.ai.sdk.entity;

import java.util.List;

/**
 * Represents an embedding vector.
 */
public record Vector(String id, List<Float> values) {
}