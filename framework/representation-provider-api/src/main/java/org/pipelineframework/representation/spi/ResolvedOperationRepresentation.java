package org.pipelineframework.representation.spi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable compiler result for one selected operation representation. */
public record ResolvedOperationRepresentation(
    String providerKey,
    String boundaryIdentity,
    OperationRepresentationRole role,
    String mappingKey,
    CanonicalType canonicalType,
    String mode,
    Optional<String> representationType,
    Optional<String> mapperType,
    String mappingFingerprint,
    Map<String, Object> generationConfiguration,
    Optional<String> canonicalSchemaFingerprint
) {
    public ResolvedOperationRepresentation(String providerKey, String boundaryIdentity, OperationRepresentationRole role,
        String mappingKey, CanonicalType canonicalType, String mode, Optional<String> representationType,
        Optional<String> mapperType, String mappingFingerprint, Map<String, Object> generationConfiguration) {
        this(providerKey, boundaryIdentity, role, mappingKey, canonicalType, mode, representationType, mapperType,
            mappingFingerprint, generationConfiguration, Optional.empty());
    }

    public ResolvedOperationRepresentation {
        providerKey = text(providerKey, "representation provider key");
        boundaryIdentity = text(boundaryIdentity, "operation boundary identity");
        role = Objects.requireNonNull(role, "operation representation role must not be null");
        mappingKey = text(mappingKey, "operation mapping key");
        canonicalType = Objects.requireNonNull(canonicalType, "operation canonical type must not be null");
        mode = text(mode, "operation representation mode");
        representationType = optionalText(representationType, "operation representation type");
        mapperType = optionalText(mapperType, "operation mapper type");
        mappingFingerprint = text(mappingFingerprint, "operation mapping fingerprint");
        if (!mappingFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("operation mapping fingerprint must be SHA-256 hex");
        }
        generationConfiguration = immutableMap(generationConfiguration);
        canonicalSchemaFingerprint = optionalText(canonicalSchemaFingerprint, "canonical schema fingerprint");
        canonicalSchemaFingerprint.ifPresent(value -> {
            if (!value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("canonical schema fingerprint must be SHA-256 hex");
        });
    }

    private static Map<String, Object> immutableMap(Map<String, Object> value) {
        Objects.requireNonNull(value, "operation generation configuration must not be null");
        Map<String, Object> result = new LinkedHashMap<>();
        value.forEach((key, child) -> {
            String normalized = text(key, "operation generation configuration key");
            if (result.containsKey(normalized)) {
                throw new IllegalArgumentException(
                    "duplicate operation generation configuration key after normalisation: " + normalized);
            }
            result.put(normalized, child);
        });
        return Collections.unmodifiableMap(result);
    }

    private static Optional<String> optionalText(Optional<String> value, String subject) {
        return Objects.requireNonNull(value, subject + " must not be null").map(item -> text(item, subject));
    }

    private static String text(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
