package org.pipelineframework.query;

import org.pipelineframework.step.NonRetryableException;

/** Non-retryable pipeline mapping of a typed Query NotFound outcome. */
public final class QueryNotFoundException extends NonRetryableException {
    private final String outcomeCode;

    public QueryNotFoundException(String outcomeCode) {
        super("query outcome not_found: " + QueryFailureCode.require(outcomeCode));
        this.outcomeCode = outcomeCode;
    }

    public String outcomeCode() {
        return outcomeCode;
    }
}
