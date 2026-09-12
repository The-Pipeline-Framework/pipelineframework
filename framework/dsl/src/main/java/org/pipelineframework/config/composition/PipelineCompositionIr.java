package org.pipelineframework.config.composition;

import java.util.List;

/**
 * Validated typed-tree composition model derived from pipeline boundary declarations.
 *
 * @param config source composition manifest
 * @param nodes resolved pipeline nodes in manifest order
 * @param handoffs derived publication handoffs
 * @param entrypointPipelineIds pipeline ids with no input subscription
 * @param terminalPublications checkpoint publications with no composed consumer
 */
public record PipelineCompositionIr(
    PipelineCompositionConfig config,
    List<PipelineCompositionNode> nodes,
    List<PipelineCompositionHandoff> handoffs,
    List<String> entrypointPipelineIds,
    List<String> terminalPublications
) {
    public PipelineCompositionIr {
        if (config == null) {
            throw new IllegalArgumentException("composition config must not be null");
        }
        nodes = nodes == null ? List.of() : List.copyOf(nodes);
        handoffs = handoffs == null ? List.of() : List.copyOf(handoffs);
        entrypointPipelineIds = entrypointPipelineIds == null ? List.of() : List.copyOf(entrypointPipelineIds);
        terminalPublications = terminalPublications == null ? List.of() : List.copyOf(terminalPublications);
    }
}
