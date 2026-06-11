package org.pipelineframework.orchestrator;

/**
 * Ordered generated-pipeline step metadata.
 *
 * @param index zero-based pipeline order index
 * @param authoredName step name from pipeline.yaml or generated model
 * @param kind authored step kind
 * @param cardinality authored cardinality
 * @param inputTypeId input domain type id
 * @param outputTypeId output domain type id
 * @param runtimeClass runtime service or await client class, when resolvable
 * @param clientClass generated client step class, when resolvable
 * @param awaitTransport await transport type, for await steps
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
    String awaitTransport
) {
}
