package org.pipelineframework.checkout.deliverorder.common.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

@ApplicationScoped
public class RuntimeProducers {

    /**
     * Produces an application-scoped Clock using the system UTC time source.
     *
     * @return the Clock configured to use system UTC.
     */
    @Produces
    @ApplicationScoped
    Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Provides a supplier that generates a new random UUID each time it is invoked and is exposed as an application-scoped CDI bean.
     *
     * @return a Supplier<UUID> that returns a new random UUID on each invocation
     */
    @Produces
    @ApplicationScoped
    Supplier<UUID> uuidSupplier() {
        return UUID::randomUUID;
    }
}