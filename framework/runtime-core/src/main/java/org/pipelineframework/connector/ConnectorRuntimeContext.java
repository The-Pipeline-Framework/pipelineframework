package org.pipelineframework.connector;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * Provider-lifetime runtime services. It must not carry mutable current-execution state.
 */
public interface ConnectorRuntimeContext {
    String runtimeIdentity();

    Executor executor();

    Clock clock();

    Optional<ConnectionResolver> connectionResolver();

    Optional<SecretResolver> secretResolver();

    static ConnectorRuntimeContext empty() {
        return of("plain-java", Runnable::run, Clock.systemUTC(), Optional.empty(), Optional.empty());
    }

    static ConnectorRuntimeContext of(
        String runtimeIdentity,
        Executor executor,
        Clock clock,
        Optional<ConnectionResolver> connectionResolver,
        Optional<SecretResolver> secretResolver
    ) {
        return new DefaultConnectorRuntimeContext(runtimeIdentity, executor, clock, connectionResolver, secretResolver);
    }
}
