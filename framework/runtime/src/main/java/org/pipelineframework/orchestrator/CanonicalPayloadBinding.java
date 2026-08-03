package org.pipelineframework.orchestrator;

import java.util.Objects;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/** Release-scoped canonical type definition and Java realization. */
public record CanonicalPayloadBinding(
    String canonicalTypeId,
    String typeExpressionFingerprint,
    String catalogFingerprint,
    Class<?> runtimeClass,
    JavaType runtimeType
) {
    public CanonicalPayloadBinding(
        String canonicalTypeId,
        String typeExpressionFingerprint,
        String catalogFingerprint,
        Class<?> runtimeClass
    ) {
        this(canonicalTypeId, typeExpressionFingerprint, catalogFingerprint, runtimeClass,
            TypeFactory.defaultInstance().constructType(runtimeClass));
    }

    public CanonicalPayloadBinding {
        require(canonicalTypeId, "canonicalTypeId");
        require(typeExpressionFingerprint, "typeExpressionFingerprint");
        require(catalogFingerprint, "catalogFingerprint");
        Objects.requireNonNull(runtimeClass, "runtimeClass");
        Objects.requireNonNull(runtimeType, "runtimeType");
        if (com.google.protobuf.Message.class.isAssignableFrom(runtimeClass)) {
            throw new IllegalArgumentException("Canonical payload binding must not target protobuf type " + runtimeClass.getName());
        }
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
