package org.pipelineframework.connector;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;

import io.quarkus.arc.DefaultBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Quarkus/CDI adapter that supplies the provider-lifetime core runtime context.
 */
@ApplicationScoped
@SuppressWarnings("removal")
public class ConnectorRuntimeContextProducer {
    @Produces
    @ApplicationScoped
    @DefaultBean
    ConnectorRuntimeContext connectorRuntimeContext(
        Instance<ConnectionResolver> connectionResolvers,
        Instance<SecretResolver> secretResolvers
    ) {
        return ConnectorRuntimeContext.of(
            "quarkus",
            Runnable::run,
            java.time.Clock.systemUTC(),
            exactlyOne(connectionResolvers, "ConnectionResolver"),
            exactlyOne(secretResolvers, "SecretResolver"));
    }

    static <T> Optional<T> exactlyOne(Iterable<T> candidates, String label) {
        List<T> resolved = new ArrayList<>();
        candidates.forEach(resolved::add);
        if (resolved.size() > 1) {
            throw new IllegalStateException("Multiple " + label + " beans are registered: "
                + resolved.stream().map(candidate -> candidate.getClass().getName()).sorted().toList());
        }
        return resolved.stream().findFirst();
    }
}
