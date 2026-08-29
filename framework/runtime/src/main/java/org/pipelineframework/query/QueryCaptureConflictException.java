package org.pipelineframework.query;

/** Conditional-write or stale-writer conflict in a Query capture store. */
public final class QueryCaptureConflictException extends QueryCaptureStoreException {
    public QueryCaptureConflictException(String message) {
        super(message);
    }

    public QueryCaptureConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
