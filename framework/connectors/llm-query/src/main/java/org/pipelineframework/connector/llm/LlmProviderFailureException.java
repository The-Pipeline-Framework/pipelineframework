package org.pipelineframework.connector.llm;

import java.util.Objects;

/** Provider-neutral, safely classifiable failure from an LLM adapter. */
public final class LlmProviderFailureException extends RuntimeException {
    private final Kind kind;
    private final String outcomeCode;

    public LlmProviderFailureException(Kind kind, String outcomeCode, Throwable cause) {
        super("LLM provider failed with safe outcome code " + requireCode(outcomeCode),
            Objects.requireNonNull(cause, "LLM provider failure cause must not be null"));
        this.kind = Objects.requireNonNull(kind, "LLM provider failure kind must not be null");
        this.outcomeCode = outcomeCode;
    }

    public Kind kind() {
        return kind;
    }

    public String outcomeCode() {
        return outcomeCode;
    }

    private static String requireCode(String value) {
        Objects.requireNonNull(value, "LLM provider outcome code must not be null");
        if (!value.matches("[a-z][a-z0-9-]{0,127}")) {
            throw new IllegalArgumentException("invalid LLM provider outcome code: " + value);
        }
        return value;
    }

    public enum Kind {
        AUTHENTICATION_REQUIRED,
        TEMPORARILY_UNAVAILABLE,
        TERMINAL
    }
}
