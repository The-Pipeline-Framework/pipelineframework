package org.pipelineframework.checkout.deliverorder.common.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

@ApplicationScoped
public class RuntimeProducers {

    @Produces
    @ApplicationScoped
    Clock clock() {
        return Clock.systemUTC();
    }

    @Produces
    @ApplicationScoped
    Supplier<UUID> uuidSupplier() {
        return UUID::randomUUID;
    }
}
