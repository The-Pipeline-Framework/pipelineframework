package org.pipelineframework.representation.spi;

import java.util.Objects;

/** A named canonical type, intentionally independent of a renderer or Java type system. */
public record CanonicalType(String name, String targetTypeName, CanonicalTypeShape shape) {
    public CanonicalType {
        name = required(name, "name");
        targetTypeName = required(targetTypeName, "targetTypeName");
        shape = Objects.requireNonNull(shape, "shape must not be null");
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
