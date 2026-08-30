package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Typed request passed to a host runtime when a connector needs an authenticated connection.
 *
 * <p>The logical reference is deployment-owned, while the invocation context is TPF-owned. The
 * resolved connection is runtime-only and must not be placed in pipeline data or generated
 * metadata.</p>
 */
public record ConnectionResolutionRequest<C extends ResolvedConnection>(
    ConnectionRef reference,
    Class<C> connectionType,
    ConnectorExecutionContext invocationContext
) {
    public ConnectionResolutionRequest {
        reference = Objects.requireNonNull(reference, "connection reference must not be null");
        connectionType = Objects.requireNonNull(connectionType, "connection type must not be null");
        invocationContext = Objects.requireNonNull(invocationContext, "connector invocation context must not be null");
    }
}
