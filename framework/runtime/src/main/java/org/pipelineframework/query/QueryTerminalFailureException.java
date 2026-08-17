package org.pipelineframework.query;

import org.pipelineframework.step.NonRetryableException;

/** Non-retryable pipeline mapping of a typed Query TerminalFailure outcome. */
public final class QueryTerminalFailureException extends NonRetryableException {
    private final String outcomeCode;

    public QueryTerminalFailureException(String outcomeCode) {
        super("query outcome terminal_failure: " + QueryFailureCode.require(outcomeCode));
        this.outcomeCode = outcomeCode;
    }

    public String outcomeCode() {
        return outcomeCode;
    }
}
