package org.pipelineframework.orchestrator;

import java.util.Map;

/**
 * Shared ordered generated-pipeline step metadata.
 *
 * @param index zero-based pipeline order index
 * @param authoredName step name from pipeline.yaml or generated model
 * @param kind authored step kind
 * @param cardinality authored cardinality
 * @param inputTypeId input domain type id
 * @param outputTypeId output domain type id
 * @param runtimeClass runtime service or completion-decorating client class, when resolvable
 * @param clientClass generated client step class, when resolvable
 * @param deferredCompletion immutable deferred-completion metadata, or an empty map
 */
public record PipelineBundleStepDescriptor(
    int index,
    String authoredName,
    String kind,
    String cardinality,
    String inputTypeId,
    String outputTypeId,
    String runtimeClass,
    String clientClass,
    Map<String, Object> deferredCompletion
) {
    public PipelineBundleStepDescriptor {
        deferredCompletion = deferredCompletion == null ? Map.of() : Map.copyOf(deferredCompletion);
    }
}
