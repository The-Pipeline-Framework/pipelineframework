package org.pipelineframework.connector.http;

public enum HttpResponseOutcome {
    RESULT,
    EMPTY,
    RETRYABLE_FAILURE,
    TERMINAL_FAILURE,
    AUTHENTICATION_REQUIRED,
    SUCCEEDED
}
