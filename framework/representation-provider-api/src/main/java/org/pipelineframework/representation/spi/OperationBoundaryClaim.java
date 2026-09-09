package org.pipelineframework.representation.spi;

import java.util.Objects;
import java.util.List;

/** Provider-owned normalized wire contracts for one selected Connector operation. */
public record OperationBoundaryClaim(
    String providerKey,
    WireBoundary request,
    List<WireBoundary> responses
) {
    public OperationBoundaryClaim {
        providerKey = text(providerKey, "representation provider key");
        request = Objects.requireNonNull(request, "operation request wire boundary must not be null");
        responses = List.copyOf(Objects.requireNonNull(responses,
            "operation response wire boundaries must not be null"));
        if (responses.isEmpty()) throw new IllegalArgumentException("operation requires at least one response wire boundary");
        if (responses.stream().map(WireBoundary::mappingKey).distinct().count() != responses.size()) {
            throw new IllegalArgumentException("operation response wire boundaries contain duplicate mapping keys");
        }
    }

    public record WireBoundary(String mappingKey, String schemaJson, String schemaFingerprint) {
        public WireBoundary {
            mappingKey = text(mappingKey, "operation representation mapping key");
            schemaJson = text(schemaJson, "operation wire schema");
            schemaFingerprint = text(schemaFingerprint, "operation wire schema fingerprint");
            if (!schemaFingerprint.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("operation wire schema fingerprint must be SHA-256 hex");
            }
        }
    }

    private static String text(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
