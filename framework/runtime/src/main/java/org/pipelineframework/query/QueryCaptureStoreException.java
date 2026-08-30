package org.pipelineframework.query;

/** Operational or persisted-data failure raised by a Query capture store. */
public class QueryCaptureStoreException extends RuntimeException {
    public QueryCaptureStoreException(String message) {
        super(message);
    }

    public QueryCaptureStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
