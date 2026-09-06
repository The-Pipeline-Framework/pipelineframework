package org.pipelineframework.connector;

import java.util.Objects;

/** Structural Java representation of {@code <tpf.connector.JsonPayload>}. */
public record JsonPayload(String contentType, String schemaHint, String bodyJson) {
    public JsonPayload {
        Objects.requireNonNull(contentType, "contentType");
        Objects.requireNonNull(schemaHint, "schemaHint");
        Objects.requireNonNull(bodyJson, "bodyJson");
    }
}
