package org.pipelineframework.connector;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * Provider-lifetime runtime services. It must not carry mutable current-execution state.
 */
@SuppressWarnings("removal")
public interface ConnectorRuntimeContext {
    String runtimeIdentity();

    Executor executor();

    Clock clock();

    Optional<ConnectionResolver> connectionResolver();

    /**
     * Returns the legacy context-free secret resolver when one is configured.
     *
     * @deprecated Connector authentication must use {@link #connectionResolver()} so resolution
     * receives the current tenant and invocation context.
     */
    @Deprecated(forRemoval = true)
    Optional<SecretResolver> secretResolver();

    /**
     * Returns a plain-Java default using the caller-thread executor ({@code Runnable::run}).
     * It does not offload blocking work, so invoking {@link #executor()} may block the calling thread.
     */
    static ConnectorRuntimeContext empty() {
        return of("plain-java", Runnable::run, Clock.systemUTC(), Optional.empty());
    }

    static ConnectorRuntimeContext of(
        String runtimeIdentity,
        Executor executor,
        Clock clock,
        Optional<ConnectionResolver> connectionResolver
    ) {
        return of(runtimeIdentity, executor, clock, connectionResolver, Optional.empty());
    }

    /**
     * @deprecated Retained for source compatibility with legacy secret resolution. New host
     * integrations should expose only a tenant-aware {@link ConnectionResolver}.
     */
    @Deprecated(forRemoval = true)
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
