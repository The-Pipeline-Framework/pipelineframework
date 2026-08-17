package org.pipelineframework.connector.llm;

import java.util.List;
import java.util.Objects;

/** Provider-neutral single inference request. */
public record LlmTurnRequest(String instructions, String applicationStateJson, List<LlmToolDefinition> tools) {
    public LlmTurnRequest {
        instructions = Objects.requireNonNull(instructions, "LLM instructions must not be null");
        applicationStateJson = Objects.requireNonNull(applicationStateJson, "application state JSON must not be null");
        tools = List.copyOf(Objects.requireNonNull(tools, "LLM tools must not be null"));
        if (tools.isEmpty()) {
            throw new IllegalArgumentException("an LLM turn must expose at least one decision alternative");
        }
    }
}
