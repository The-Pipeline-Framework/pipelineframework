package org.pipelineframework.config;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

/**
 * Optional exact-boundary circuit overrides layered over {@link PipelineCircuitDefaultsConfig}.
 */
@ConfigMapping(prefix = "pipeline.resilience")
@Unremovable
public interface PipelineCircuitOverridesConfig {

    /**
     * Exact circuit overrides keyed by stable transport-boundary identity.
     *
     * @return configured overrides
     */
    Map<String, CircuitOverrideConfig> circuit();

    interface CircuitOverrideConfig {
        /**
         * Overrides boundary enablement when present.
         *
         * @return optional enablement override
         */
        Optional<Boolean> enabled();

        /**
         * Overrides the deployment-provided circuit scope when present.
         *
         * @return optional scope override
         */
        Optional<CircuitScope> scope();

        /**
         * Overrides the failure threshold when present.
         *
         * @return optional failure threshold
         */
        @WithName("failure-threshold")
        Optional<Integer> failureThreshold();

        /**
         * Overrides the failure window when present.
         *
         * @return optional failure window
         */
        @WithName("failure-window")
        Optional<Duration> failureWindow();

        /**
         * Overrides the open duration when present.
         *
         * @return optional open duration
         */
        @WithName("open-duration")
        Optional<Duration> openDuration();

        /**
         * Overrides the half-open probe limit when present.
         *
         * @return optional half-open permit limit
         */
        @WithName("half-open-max-permits")
        Optional<Integer> halfOpenMaxPermits();

        /**
         * Overrides the saturated half-open retry hint when present.
         *
         * @return optional half-open retry delay
         */
        @WithName("half-open-retry-delay")
        Optional<Duration> halfOpenRetryDelay();

        /**
         * Overrides the shared half-open probe lease duration when present.
         *
         * @return optional probe lease duration
         */
        @WithName("half-open-probe-lease-duration")
        Optional<Duration> halfOpenProbeLeaseDuration();

        /**
         * Groups compatible boundaries under one logical circuit identity when present.
         *
         * @return optional logical identity
         */
        Optional<String> identity();
    }
}
