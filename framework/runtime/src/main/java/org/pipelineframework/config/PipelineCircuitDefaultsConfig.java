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

    /**
     * Enables circuit admission for eligible managed transport boundaries.
     *
     * @return whether circuit admission is enabled
     */
    @WithDefault("true")
    boolean enabled();

    /**
     * Sets the number of health failures required to open a circuit.
     *
     * @return failure threshold
     */
    @WithName("failure-threshold")
    @WithDefault("5")
    int failureThreshold();

    /**
     * Sets the rolling period in which health failures are counted.
     *
     * @return failure window
     */
    @WithName("failure-window")
    @WithDefault("PT1M")
    Duration failureWindow();

    /**
     * Sets the minimum interval for which an open circuit rejects calls.
     *
     * @return open duration
     */
    @WithName("open-duration")
    @WithDefault("PT30S")
    Duration openDuration();

    /**
     * Limits concurrent recovery probes in half-open state.
     *
     * @return maximum half-open permits
     */
    @WithName("half-open-max-permits")
    @WithDefault("1")
    int halfOpenMaxPermits();

    /**
     * Sets the scheduling hint returned when half-open probe capacity is saturated.
     *
     * @return half-open retry delay
     */
    @WithName("half-open-retry-delay")
    @WithDefault("PT1S")
    Duration halfOpenRetryDelay();

    /**
     * Sets the shared half-open probe lease duration.
     *
     * @return half-open probe lease duration
     */
    @WithName("half-open-probe-lease-duration")
    @WithDefault("PT30S")
    Duration halfOpenProbeLeaseDuration();
}
