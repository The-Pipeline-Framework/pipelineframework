package org.pipelineframework.orchestrator;

import java.util.List;

/**
 * Runtime capabilities declared by a generated pipeline bundle.
 *
 * @param localTransitionExecution whether the bundle can execute transitions in-process
 * @param transitionWorkerProtocols supported transition worker protocols
 */
public record PipelineBundleCapabilities(
    boolean localTransitionExecution,
    List<String> transitionWorkerProtocols
) {
    public PipelineBundleCapabilities {
        transitionWorkerProtocols = transitionWorkerProtocols == null
            ? List.of()
            : List.copyOf(transitionWorkerProtocols);
    }

    /**
     * Default capabilities for the current app-hosted runtime.
     *
     * @return default capabilities
     */
    public static PipelineBundleCapabilities defaults() {
        return new PipelineBundleCapabilities(true, List.of("local", "rest", "grpc", "sqs"));
    }
}
