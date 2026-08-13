package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Provider-level mechanics shared by every operation the provider exposes.
 */
public record ConnectorExecutionCapabilities(
    ConnectorExecutionStyle executionStyle,
    ConnectorConcurrencyScope concurrencyScope
) {
    public ConnectorExecutionCapabilities {
        executionStyle = Objects.requireNonNull(executionStyle, "connector execution style must not be null");
        concurrencyScope = Objects.requireNonNull(concurrencyScope, "connector concurrency scope must not be null");
    }

    public static ConnectorExecutionCapabilities conservative() {
        return new ConnectorExecutionCapabilities(ConnectorExecutionStyle.UNSPECIFIED, ConnectorConcurrencyScope.UNSPECIFIED);
    }
}
