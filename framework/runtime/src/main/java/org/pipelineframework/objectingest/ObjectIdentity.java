package org.pipelineframework.objectingest;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.pipelineframework.config.boundary.PipelineObjectIdentityConfig;

/**
 * Deterministic object identity helper for async execution idempotency.
 */
public final class ObjectIdentity {

    private ObjectIdentity() {
    }

    public static String executionKey(String sourceName, ObjectSnapshot snapshot, PipelineObjectIdentityConfig config) {
        Objects.requireNonNull(sourceName, "sourceName");
        Objects.requireNonNull(snapshot, "snapshot");
        List<String> fields = config == null ? PipelineObjectIdentityConfig.defaults().fields() : config.fields();
        List<String> values = new ArrayList<>();
        for (String field : fields) {
            values.add(valueFor(sourceName, snapshot, field));
        }
        return "object:" + escape(sourceName) + ":" + String.join(":", values);
    }

    private static String valueFor(String sourceName, ObjectSnapshot snapshot, String field) {
        String value = switch (field) {
            case "source", "sourceName" -> sourceName;
            case "provider" -> snapshot.provider();
            case "container", "bucket" -> snapshot.container();
            case "key" -> snapshot.key();
            case "version", "versionId" -> snapshot.versionId();
            case "etag" -> snapshot.etag();
            default -> snapshot.metadata().get(field);
        };
        return escape(value);
    }

    private static String escape(String value) {
        if (value == null || value.isBlank()) {
            return "_";
        }
        return value.trim().replace("\\", "\\\\").replace(":", "\\:");
    }
}
