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

    Map<String, CircuitOverrideConfig> circuit();

    interface CircuitOverrideConfig {
        Optional<Boolean> enabled();

        Optional<CircuitScope> scope();

        @WithName("failure-threshold")
        Optional<Integer> failureThreshold();

        @WithName("failure-window")
        Optional<Duration> failureWindow();

        @WithName("open-duration")
        Optional<Duration> openDuration();

        @WithName("half-open-max-permits")
        Optional<Integer> halfOpenMaxPermits();

        @WithName("half-open-retry-delay")
        Optional<Duration> halfOpenRetryDelay();

        @WithName("half-open-probe-lease-duration")
        Optional<Duration> halfOpenProbeLeaseDuration();

        Optional<String> identity();
    }
}
