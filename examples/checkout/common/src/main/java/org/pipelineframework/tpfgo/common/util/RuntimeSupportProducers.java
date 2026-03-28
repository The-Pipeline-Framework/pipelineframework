package org.pipelineframework.tpfgo.common.util;

import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class RuntimeSupportProducers {

    @Produces
    Clock systemClock() {
        return Clock.systemUTC();
    }

    @Produces
    Supplier<UUID> uuidSupplier() {
        return UUID::randomUUID;
    }
}
