package org.pipelineframework.representation.spi;

import java.util.Objects;
import java.util.List;

/** Provider-owned normalized wire contracts for one selected Connector operation. */
public record OperationBoundaryClaim(
    String providerKey,
    WireBoundary request,
    List<WireBoundary> responses,
    List<CallbackBoundary> callbacks
) {
    public OperationBoundaryClaim(String providerKey, WireBoundary request, List<WireBoundary> responses) {
        this(providerKey, request, responses, List.of());
    }

    public OperationBoundaryClaim {
        providerKey = text(providerKey, "representation provider key");
        request = Objects.requireNonNull(request, "operation request wire boundary must not be null");
        responses = List.copyOf(Objects.requireNonNull(responses,
            "operation response wire boundaries must not be null"));
        if (responses.isEmpty()) throw new IllegalArgumentException("operation requires at least one response wire boundary");
        if (responses.stream().map(WireBoundary::mappingKey).distinct().count() != responses.size()) {
            throw new IllegalArgumentException("operation response wire boundaries contain duplicate mapping keys");
        }
        callbacks = List.copyOf(Objects.requireNonNull(callbacks, "operation callback wire boundaries"));
        if (callbacks.size() > 16 || callbacks.stream().map(CallbackBoundary::id).distinct().count() != callbacks.size()) {
            throw new IllegalArgumentException("operation callback boundaries must be bounded and distinct");
        }
    }

    public record CallbackBoundary(String id, String canonicalType, WireBoundary wire) {
        public CallbackBoundary {
            id = text(id, "callback identity");
            canonicalType = text(canonicalType, "callback canonical type");
            wire = Objects.requireNonNull(wire, "callback wire boundary");
            if (!wire.runtimeSuppliedPaths().isEmpty()) {
                throw new IllegalArgumentException("inbound callbacks cannot contain runtime-supplied request fields");
            }
        }
    }

    public record WireBoundary(String mappingKey, String schemaJson, String schemaFingerprint,
        List<String> runtimeSuppliedPaths) {
        public WireBoundary(String mappingKey, String schemaJson, String schemaFingerprint) {
            this(mappingKey, schemaJson, schemaFingerprint, List.of());
        }

        public WireBoundary {
            mappingKey = text(mappingKey, "operation representation mapping key");
            schemaJson = text(schemaJson, "operation wire schema");
            schemaFingerprint = text(schemaFingerprint, "operation wire schema fingerprint");
            if (!schemaFingerprint.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("operation wire schema fingerprint must be SHA-256 hex");
            }
            runtimeSuppliedPaths = normalizeRuntimePaths(runtimeSuppliedPaths);
        }

        static List<String> normalizeRuntimePaths(List<String> paths) {
            Objects.requireNonNull(paths, "runtime-supplied paths");
            if (paths.size() > 128 || paths.stream().anyMatch(path -> path == null || !path.startsWith("/")
                || path.length() > 4096 || path.matches(".*~(?![01]).*")
                || path.codePoints().anyMatch(Character::isISOControl))) {
                throw new IllegalArgumentException("runtime-supplied paths must be bounded JSON Pointers");
            }
            return paths.stream().distinct().sorted().toList();
        }
    }

    private static String text(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
