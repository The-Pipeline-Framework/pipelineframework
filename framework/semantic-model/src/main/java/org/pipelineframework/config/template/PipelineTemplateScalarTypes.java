package org.pipelineframework.config.template;

import java.util.Locale;
import java.util.Set;

/** Host-neutral scalar vocabulary shared by v3 type authors and compiler adapters. */
public final class PipelineTemplateScalarTypes {
    static final Set<String> TYPES = Set.of(
        "string", "bool", "int32", "int64", "float32", "float64", "decimal", "uuid",
        "timestamp", "datetime", "date", "duration", "bytes", "currency", "uri", "path", "payload_ref");

    private PipelineTemplateScalarTypes() {
    }

    public static boolean isScalar(String value) {
        return value != null && TYPES.contains(value.trim().toLowerCase(Locale.ROOT));
    }
}
