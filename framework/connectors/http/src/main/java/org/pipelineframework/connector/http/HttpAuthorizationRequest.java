package org.pipelineframework.connector.http;

import java.net.URI;
import java.util.Objects;

import org.pipelineframework.connector.ConnectorExecutionContext;

/** Host authorization request containing descriptive constraints but no OpenAPI document. */
public record HttpAuthorizationRequest(
    URI baseUri,
    String operation,
    HttpSecurityConstraint security,
    ConnectorExecutionContext executionContext
) {
    public HttpAuthorizationRequest {
        baseUri = Objects.requireNonNull(baseUri, "HTTP base URI must not be null");
        operation = Objects.requireNonNull(operation, "HTTP operation must not be null");
        security = Objects.requireNonNull(security, "HTTP security constraint must not be null");
        executionContext = Objects.requireNonNull(executionContext, "HTTP execution context must not be null");
    }
}
