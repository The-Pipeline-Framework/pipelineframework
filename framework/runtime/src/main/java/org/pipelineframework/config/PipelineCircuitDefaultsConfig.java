package org.pipelineframework.config;

import java.time.Duration;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Circuit policy in the existing {@code pipeline.defaults} configuration hierarchy.
 * Scope and backend selection remain deployment concerns.
 */
@ConfigMapping(prefix = "pipeline.defaults.circuit")
@Unremovable
public interface PipelineCircuitDefaultsConfig {

    @WithDefault("true")
    boolean enabled();

    @WithName("failure-threshold")
    @WithDefault("5")
    int failureThreshold();

    @WithName("failure-window")
    @WithDefault("PT1M")
    Duration failureWindow();

    @WithName("open-duration")
    @WithDefault("PT30S")
    Duration openDuration();

    @WithName("half-open-max-permits")
    @WithDefault("1")
    int halfOpenMaxPermits();

    @WithName("half-open-retry-delay")
    @WithDefault("PT1S")
    Duration halfOpenRetryDelay();

    @WithName("half-open-probe-lease-duration")
    @WithDefault("PT30S")
    Duration halfOpenProbeLeaseDuration();
}
