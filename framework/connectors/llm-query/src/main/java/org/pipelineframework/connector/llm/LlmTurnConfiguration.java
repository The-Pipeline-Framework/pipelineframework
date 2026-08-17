package org.pipelineframework.connector.llm;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Operation-owned prompt and compiler-validated release catalogue. */
public record LlmTurnConfiguration(
    String instructions,
    Map<String, LlmCallableConfiguration> callables,
    Optional<StructuredOutputSchemaMode> structuredOutputSchema
) {
    public LlmTurnConfiguration {
        instructions = Objects.requireNonNull(instructions, "LLM instructions must not be null").trim();
        if (instructions.isEmpty()) {
            throw new IllegalArgumentException("LLM instructions must not be blank");
        }
        callables = callables == null ? Map.of() : Map.copyOf(callables);
        structuredOutputSchema = Objects.requireNonNull(
            structuredOutputSchema, "structured output schema mode must not be null");
        callables.keySet().forEach(alias -> {
            if (!alias.matches("[a-z][a-z0-9]*(?:[_-][a-z0-9]+)*")) {
                throw new IllegalArgumentException("LLM callable alias must be a lowercase model-safe token: " + alias);
            }
        });
    }

    public LlmTurnConfiguration(String instructions, Map<String, LlmCallableConfiguration> callables) {
        this(instructions, callables, Optional.empty());
    }

    public LlmTurnConfiguration(
        String instructions,
        Map<String, LlmCallableConfiguration> callables,
        StructuredOutputSchemaMode structuredOutputSchema
    ) {
        this(instructions, callables, Optional.of(Objects.requireNonNull(
            structuredOutputSchema, "structured output schema mode must not be null")));
    }

    public StructuredOutputSchemaMode structuredOutputMode() {
        return structuredOutputSchema.orElse(StructuredOutputSchemaMode.REQUIRED);
    }
}
