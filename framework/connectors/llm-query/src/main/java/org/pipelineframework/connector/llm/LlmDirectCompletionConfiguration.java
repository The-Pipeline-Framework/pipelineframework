package org.pipelineframework.connector.llm;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Projects a model-authored completion into one field while carrying trusted input fields forward. */
public record LlmDirectCompletionConfiguration(
    String field,
    Optional<Map<String, String>> carry
) {
    public LlmDirectCompletionConfiguration {
        field = requirePath(field, "LLM direct completion field");
        carry = Objects.requireNonNull(carry, "LLM direct completion carry map must not be null")
            .map(Map::copyOf);
        carry.orElseGet(Map::of).forEach((outputField, inputPath) -> {
            requirePath(outputField, "LLM direct completion output field");
            requirePath(inputPath, "LLM direct completion input path");
        });
    }

    public LlmDirectCompletionConfiguration(String field, Map<String, String> carry) {
        this(field, Optional.ofNullable(carry));
    }

    public Map<String, String> carriedFields() {
        return carry.orElseGet(Map::of);
    }

    private static String requirePath(String value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        value = value.trim();
        if (!value.matches("[A-Za-z][A-Za-z0-9]*(?:\\.[A-Za-z][A-Za-z0-9]*)*")) {
            throw new IllegalArgumentException(label + " must be a dotted field path: " + value);
        }
        return value;
    }
}
