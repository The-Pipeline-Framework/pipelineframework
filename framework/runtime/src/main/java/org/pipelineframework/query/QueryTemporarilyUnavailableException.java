package org.pipelineframework.query;

/** Retryable pipeline mapping of a typed Query TemporarilyUnavailable outcome. */
public final class QueryTemporarilyUnavailableException extends RuntimeException {
    private final String outcomeCode;

    public QueryTemporarilyUnavailableException(String outcomeCode) {
        super("query outcome temporarily_unavailable: " + QueryFailureCode.require(outcomeCode));
        this.outcomeCode = outcomeCode;
    }

    public String outcomeCode() {
        return outcomeCode;
    }
}
