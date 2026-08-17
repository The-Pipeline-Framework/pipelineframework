package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/** Normalized, model-safe input and output type names for a connector operation. */
public record ConnectorOperationTypeContract(String inputType, Optional<String> outputType) {
    public ConnectorOperationTypeContract {
        inputType = requireType(inputType, "connector operation input type");
        outputType = Objects.requireNonNull(outputType, "connector operation output type must not be null")
            .map(value -> requireType(value, "connector operation output type"));
    }

    private static String requireType(String value, String subject) {
        Objects.requireNonNull(value, subject + " must not be null");
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(subject + " must not be blank");
        }
        if (normalized.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(subject + " must not contain control characters");
        }
        return normalized;
    }
}
