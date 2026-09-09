package org.pipelineframework.connector.http;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectorProviderId;

/** Compiler-resolved deterministic representation selected for an HTTP operation boundary. */
public record HttpOperationRepresentationBinding(
    String mappingKey,
    HttpRepresentationMode mode,
    Optional<String> representationType,
    Optional<String> mapperType,
    String mappingFingerprint
) {
    public HttpOperationRepresentationBinding {
        mappingKey = ConnectorProviderId.of(mappingKey).value();
        mode = Objects.requireNonNull(mode, "HTTP representation mode must not be null");
        representationType = text(representationType, "HTTP representation type");
        mapperType = text(mapperType, "HTTP mapper type");
        mappingFingerprint = Objects.requireNonNull(mappingFingerprint,
            "HTTP mapping fingerprint must not be null").trim().toLowerCase(java.util.Locale.ROOT);
        if (!mappingFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("HTTP mapping fingerprint must be SHA-256 hex");
        }
        if (mode == HttpRepresentationMode.DIRECT && (representationType.isPresent() || mapperType.isPresent())) {
            throw new IllegalArgumentException("direct HTTP representation cannot name a representation or mapper type");
        }
        if (mode != HttpRepresentationMode.DIRECT && (representationType.isEmpty() || mapperType.isEmpty())) {
            throw new IllegalArgumentException(
                "generated and curated HTTP representations require representation and mapper types");
        }
    }

    private static Optional<String> text(Optional<String> value, String subject) {
        return Objects.requireNonNull(value, subject + " must not be null").map(item -> {
            String result = item.trim();
            if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
            return result;
        });
    }
}
