package org.pipelineframework.config.boundary;

/**
 * Output boundary configuration for a pipeline.
 *
 * @param checkpoint stable checkpoint publication settings
 */
public record PipelineOutputBoundaryConfig(
    PipelineCheckpointConfig checkpoint
) {
}
