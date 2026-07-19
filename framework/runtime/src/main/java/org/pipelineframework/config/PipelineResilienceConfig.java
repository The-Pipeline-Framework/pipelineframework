package org.pipelineframework.config;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

/**
 * Opt-in runtime configuration for transport-boundary resilience policies.
 */
@ConfigMapping(prefix = "pipeline.resilience")
@Unremovable
public interface PipelineResilienceConfig {

    /**
     * Circuit settings keyed by stable {@code protocol:target} transport-boundary identity.
     *
     * @return configured circuit settings
     */
    Map<String, CircuitConfig> circuit();

    interface CircuitConfig {
        /**
         * Enables this circuit boundary.
         *
         * @return whether the circuit is enabled
         */
        @WithDefault("false")
        boolean enabled();

        /**
         * Required visibility guarantee for circuit state.
         *
         * @return the requested circuit scope
         */
        @WithDefault("LOCAL_PROCESS")
        CircuitScope scope();

        /**
         * Number of health-affecting failures required to open the circuit.
         *
         * @return failure threshold
         */
        @WithName("failure-threshold")
        @WithDefault("5")
        int failureThreshold();

        /**
         * Rolling window in which failures contribute to the threshold.
         *
         * @return failure window
         */
        @WithName("failure-window")
        @WithDefault("PT1M")
        Duration failureWindow();

        /**
         * Duration for which the circuit rejects calls after opening.
         *
         * @return open duration
         */
        @WithName("open-duration")
        @WithDefault("PT30S")
        Duration openDuration();

        /**
         * Maximum concurrent probes admitted while the circuit is half-open.
         *
         * @return maximum half-open permits
         */
        @WithName("half-open-max-permits")
        @WithDefault("1")
        int halfOpenMaxPermits();

        /**
         * Minimum spacing hint returned when half-open permits are saturated.
         *
         * @return half-open retry delay
         */
        @WithName("half-open-retry-delay")
        @WithDefault("PT1S")
        Duration halfOpenRetryDelay();

        /**
         * Optional stable logical identity used to group compatible boundaries.
         *
         * @return configured logical identity when supplied
         */
        Optional<String> identity();
    }
}
