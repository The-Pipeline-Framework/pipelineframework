package org.pipelineframework.connector.llm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Projects a model-authored completion into one field while carrying trusted input fields forward. */
public record LlmDirectCompletionConfiguration(
    String field,
    Optional<Map<String, String>> carry
) {
    public LlmDirectCompletionConfiguration {
        field = requireComponent(field, "LLM direct completion field");
        carry = Objects.requireNonNull(carry, "LLM direct completion carry map must not be null")
            .map(entries -> {
                Map<String, String> normalized = new LinkedHashMap<>();
                entries.forEach((outputField, inputPath) -> {
                    String component = requireComponent(outputField, "LLM direct completion output field");
                    String path = requirePath(inputPath, "LLM direct completion input path");
                    if (normalized.putIfAbsent(component, path) != null) {
                        throw new IllegalArgumentException(
                            "LLM direct completion output field is duplicated after normalization: " + component);
                    }
                });
                return Map.copyOf(normalized);
            });
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

    private static String requireComponent(String value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        value = value.trim();
        if (!value.matches("[A-Za-z][A-Za-z0-9]*")) {
            throw new IllegalArgumentException(label + " must be a record component name: " + value);
        }
        return value;
    }
}
