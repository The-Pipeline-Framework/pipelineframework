package org.pipelineframework.config.boundary;

import java.time.Duration;

/**
 * Object source listing poll configuration.
 *
 * @param enabled whether the runtime poller should start
 * @param interval interval between listing attempts
 * @param batchSize maximum objects listed in one attempt
 */
public record PipelineObjectPollConfig(
    boolean enabled,
    Duration interval,
    int batchSize
) {
    public PipelineObjectPollConfig {
        interval = interval == null ? Duration.ofSeconds(30) : interval;
        if (!interval.isPositive()) {
            throw new IllegalArgumentException("object source poll.interval must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("object source poll.batchSize must be positive");
        }
    }

    public static PipelineObjectPollConfig defaults() {
        return new PipelineObjectPollConfig(false, Duration.ofSeconds(30), 100);
    }
}
