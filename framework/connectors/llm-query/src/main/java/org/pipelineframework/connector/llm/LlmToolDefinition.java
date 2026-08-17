package org.pipelineframework.connector.llm;

import java.util.Objects;

/** Model-safe projection of one release-pinned callable or completion alternative. */
public record LlmToolDefinition(String alias, String description, String inputSchemaJson) {
    public LlmToolDefinition {
        alias = Objects.requireNonNull(alias, "tool alias must not be null");
        description = Objects.requireNonNull(description, "tool description must not be null");
        inputSchemaJson = Objects.requireNonNull(inputSchemaJson, "tool input schema must not be null");
    }
}
