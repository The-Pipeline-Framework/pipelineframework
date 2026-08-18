package org.pipelineframework.connector.llm;

import java.util.List;
import java.util.Objects;
import org.pipelineframework.connector.MaterializedPayload;

/** Provider-neutral single inference request. */
public record LlmTurnRequest(
    String instructions,
    String applicationStateJson,
    List<MaterializedPayload> media,
    List<LlmToolDefinition> tools,
    StructuredOutputSchemaMode structuredOutputSchema
) {
    public LlmTurnRequest {
        instructions = Objects.requireNonNull(instructions, "LLM instructions must not be null");
        applicationStateJson = Objects.requireNonNull(applicationStateJson, "application state JSON must not be null");
        media = List.copyOf(Objects.requireNonNull(media, "LLM media must not be null"));
        tools = List.copyOf(Objects.requireNonNull(tools, "LLM tools must not be null"));
        structuredOutputSchema = Objects.requireNonNull(
            structuredOutputSchema, "structured output schema mode must not be null");
        if (tools.isEmpty()) {
            throw new IllegalArgumentException("an LLM turn must expose at least one decision alternative");
        }
    }

    public LlmTurnRequest(String instructions, String applicationStateJson, List<LlmToolDefinition> tools) {
        this(instructions, applicationStateJson, List.of(), tools, StructuredOutputSchemaMode.REQUIRED);
    }

    public LlmTurnRequest(
        String instructions,
        String applicationStateJson,
        List<LlmToolDefinition> tools,
        StructuredOutputSchemaMode structuredOutputSchema
    ) {
        this(instructions, applicationStateJson, List.of(), tools, structuredOutputSchema);
    }
}
