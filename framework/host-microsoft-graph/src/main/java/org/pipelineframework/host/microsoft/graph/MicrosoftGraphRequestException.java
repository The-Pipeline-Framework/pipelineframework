package org.pipelineframework.host.microsoft.graph;

import java.io.IOException;

/** A bounded Graph request failed without exposing the provider response body. */
public final class MicrosoftGraphRequestException extends IOException {
    private final int statusCode;

    MicrosoftGraphRequestException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int statusCode() { return statusCode; }
}
