package org.pipelineframework.connector;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;

@SuppressWarnings("removal")
record DefaultConnectorRuntimeContext(
    String runtimeIdentity,
    Executor executor,
    Clock clock,
    Optional<ConnectionResolver> connectionResolver,
    Optional<SecretResolver> secretResolver
) implements ConnectorRuntimeContext {
    DefaultConnectorRuntimeContext {
        runtimeIdentity = Objects.requireNonNull(runtimeIdentity, "runtime identity must not be null");
        if (runtimeIdentity.isBlank()) {
            throw new IllegalArgumentException("runtime identity must not be blank");
        }
        executor = Objects.requireNonNull(executor, "executor must not be null");
        clock = Objects.requireNonNull(clock, "clock must not be null");
        connectionResolver = Objects.requireNonNull(connectionResolver, "connection resolver must not be null");
        secretResolver = Objects.requireNonNull(secretResolver, "secret resolver must not be null");
    }
}
