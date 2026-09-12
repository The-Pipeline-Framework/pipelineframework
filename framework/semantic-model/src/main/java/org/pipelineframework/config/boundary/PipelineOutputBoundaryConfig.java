package org.pipelineframework.config.boundary;

/**
 * Output boundary configuration for a pipeline.
 *
 * @param checkpoint stable checkpoint publication settings
 * @param object object publish settings
 */
public record PipelineOutputBoundaryConfig(
    PipelineCheckpointConfig checkpoint,
    PipelineObjectOutputConfig object
) {
    public PipelineOutputBoundaryConfig(PipelineCheckpointConfig checkpoint) {
        this(checkpoint, null);
    }

    public PipelineOutputBoundaryConfig {
        if (checkpoint != null && object != null) {
            throw new IllegalArgumentException("pipeline output boundary cannot declare both checkpoint and object");
        }
        if (checkpoint == null && object == null) {
            throw new IllegalArgumentException("pipeline output boundary must declare either checkpoint or object");
        }
    }
}
