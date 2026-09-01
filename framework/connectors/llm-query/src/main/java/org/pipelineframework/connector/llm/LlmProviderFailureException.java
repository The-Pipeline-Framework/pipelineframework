package org.pipelineframework.connector.llm;

import java.util.Set;
import java.util.Objects;

/** Provider-neutral, safely classifiable failure from an LLM adapter. */
public final class LlmProviderFailureException extends RuntimeException {
    public static final String CODE_AUTHENTICATION_REQUIRED = "llm-provider-authentication-required";
    public static final String CODE_RATE_LIMITED = "llm-provider-rate-limited";
    public static final String CODE_TIMEOUT = "llm-provider-timeout";
    public static final String CODE_UNAVAILABLE = "llm-provider-unavailable";
    public static final String CODE_MODEL_UNAVAILABLE = "llm-provider-model-unavailable";
    public static final String CODE_CONTENT_FILTERED = "llm-provider-content-filtered";
    public static final String CODE_REQUEST_REJECTED = "llm-provider-request-rejected";
    public static final String CODE_QUOTA_EXHAUSTED = "llm-provider-quota-exhausted";
    public static final String CODE_FAILED = "llm-provider-failed";

    private static final Set<String> FRAMEWORK_OUTCOME_CODES = Set.of(
        CODE_AUTHENTICATION_REQUIRED,
        CODE_RATE_LIMITED,
        CODE_TIMEOUT,
        CODE_UNAVAILABLE,
        CODE_MODEL_UNAVAILABLE,
        CODE_CONTENT_FILTERED,
        CODE_REQUEST_REJECTED,
        CODE_QUOTA_EXHAUSTED,
        CODE_FAILED);

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
        if (!FRAMEWORK_OUTCOME_CODES.contains(value)) {
            throw new IllegalArgumentException("unsupported LLM provider outcome code");
        }
        return value;
    }

    public enum Kind {
        AUTHENTICATION_REQUIRED,
        TEMPORARILY_UNAVAILABLE,
        TERMINAL
    }
}
