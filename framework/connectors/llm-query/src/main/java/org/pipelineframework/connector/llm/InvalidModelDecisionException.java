package org.pipelineframework.connector.llm;

/** Malformed, unknown, or contract-incompatible model output; distinct from provider failure. */
public final class InvalidModelDecisionException extends IllegalArgumentException {
    public InvalidModelDecisionException(String message) {
        super(message);
    }

    public InvalidModelDecisionException(String message, Throwable cause) {
        super(message, cause);
    }
}
