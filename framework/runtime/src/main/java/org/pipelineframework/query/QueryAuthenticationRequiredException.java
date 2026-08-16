package org.pipelineframework.query;

import org.pipelineframework.step.NonRetryableException;

/** Non-retryable pipeline mapping of a typed Query AuthenticationRequired outcome. */
public final class QueryAuthenticationRequiredException extends NonRetryableException {
    private final String outcomeCode;

    public QueryAuthenticationRequiredException(String outcomeCode) {
        super("query outcome authentication_required: " + QueryFailureCode.require(outcomeCode));
        this.outcomeCode = outcomeCode;
    }

    public String outcomeCode() {
        return outcomeCode;
    }
}
