package org.pipelineframework.checkout.createorder.common.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.Clock;

@ApplicationScoped
public class ClockProducer {

    /**
     * Provides an application-scoped {@link Clock} set to UTC for injection.
     *
     * @return the system {@link Clock} configured to UTC
     */
    @Produces
    @ApplicationScoped
    Clock clock() {
        return Clock.systemUTC();
    }
}