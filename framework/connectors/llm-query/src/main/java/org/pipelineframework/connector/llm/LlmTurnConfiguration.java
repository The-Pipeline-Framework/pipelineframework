package org.pipelineframework.connector.llm;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Operation-owned prompt and compiler-validated release catalogue. */
public record LlmTurnConfiguration(
    String instructions,
    Optional<Map<String, LlmCallableConfiguration>> callables,
    Optional<StructuredOutputSchemaMode> structuredOutputSchema,
    Optional<Map<String, String>> completion,
    Optional<Map<String, String>> modelInputExcludes
) {
    public LlmTurnConfiguration {
        instructions = Objects.requireNonNull(instructions, "LLM instructions must not be null").trim();
        if (instructions.isEmpty()) {
            throw new IllegalArgumentException("LLM instructions must not be blank");
        }
        callables = Objects.requireNonNull(callables, "LLM callables must not be null")
            .map(Map::copyOf);
        structuredOutputSchema = Objects.requireNonNull(
            structuredOutputSchema, "structured output schema mode must not be null");
        completion = Objects.requireNonNull(completion, "LLM direct completion configuration must not be null")
            .map(Map::copyOf);
        modelInputExcludes = Objects.requireNonNull(modelInputExcludes, "LLM model input exclusions must not be null")
            .map(Map::copyOf);
        modelInputExcludes.orElseGet(Map::of).values().forEach(path -> {
            if (path == null || !path.matches("[A-Za-z][A-Za-z0-9]*(?:\\.[A-Za-z][A-Za-z0-9]*)*")) {
                throw new IllegalArgumentException("LLM model input exclusion must be a dotted field path: " + path);
            }
        });
        callables.orElseGet(Map::of).keySet().forEach(alias -> {
            if (!alias.matches("[a-z][a-z0-9]*(?:[_-][a-z0-9]+)*")) {
                throw new IllegalArgumentException("LLM callable alias must be a lowercase model-safe token: " + alias);
            }
        });
    }

    public LlmTurnConfiguration(String instructions, Map<String, LlmCallableConfiguration> callables) {
        this(instructions, Optional.ofNullable(callables), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public LlmTurnConfiguration(
        String instructions,
        Map<String, LlmCallableConfiguration> callables,
        StructuredOutputSchemaMode structuredOutputSchema
    ) {
        this(instructions, Optional.ofNullable(callables), Optional.of(Objects.requireNonNull(
            structuredOutputSchema, "structured output schema mode must not be null")), Optional.empty(), Optional.empty());
    }

    public LlmTurnConfiguration(
        String instructions,
        Optional<Map<String, LlmCallableConfiguration>> callables,
        Optional<StructuredOutputSchemaMode> structuredOutputSchema
    ) {
        this(instructions, callables, structuredOutputSchema, Optional.empty(), Optional.empty());
    }

    public LlmTurnConfiguration(
        String instructions,
        Optional<Map<String, LlmCallableConfiguration>> callables,
        Optional<StructuredOutputSchemaMode> structuredOutputSchema,
        Optional<Map<String, String>> completion
    ) {
        this(instructions, callables, structuredOutputSchema, completion, Optional.empty());
    }

    public Map<String, LlmCallableConfiguration> callableCatalogue() {
        return callables.orElseGet(Map::of);
    }

    public StructuredOutputSchemaMode structuredOutputMode() {
        return structuredOutputSchema.orElse(StructuredOutputSchemaMode.REQUIRED);
    }

    public Optional<LlmDirectCompletionConfiguration> directCompletion() {
        return completion.map(values -> {
            String field = values.get("field");
            Map<String, String> carry = new java.util.LinkedHashMap<>(values);
            carry.remove("field");
            return new LlmDirectCompletionConfiguration(field, carry);
        });
    }

    public java.util.List<String> excludedModelInputPaths() {
        return java.util.List.copyOf(modelInputExcludes.orElseGet(Map::of).values());
    }
}
