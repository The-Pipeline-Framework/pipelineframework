package org.pipelineframework.config.boundary;

/**
 * Grouping limits for terminal Object Publish streams.
 *
 * @param maxOpenGroups maximum number of object groups that may be open at once
 */
public record PipelineObjectPublishGroupingConfig(int maxOpenGroups) {
    private static final int DEFAULT_MAX_OPEN_GROUPS = 32;

    public PipelineObjectPublishGroupingConfig {
        if (maxOpenGroups <= 0) {
            throw new IllegalArgumentException("publish.grouping.maxOpenGroups must be > 0");
        }
    }

    public static PipelineObjectPublishGroupingConfig defaults() {
        return new PipelineObjectPublishGroupingConfig(DEFAULT_MAX_OPEN_GROUPS);
    }
}
