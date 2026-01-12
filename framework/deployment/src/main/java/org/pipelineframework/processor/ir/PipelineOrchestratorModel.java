package org.pipelineframework.processor.ir;

import java.util.Set;

/**
 * Contains only semantic information derived from @PipelineOrchestrator annotations.
 * This class captures all the essential information needed to generate orchestrator artifacts.
 *
 * @param serviceName Gets the name of the orchestrator service.
 * @param servicePackage Gets the package of the orchestrator service.
 * @param enabledTargets Gets the set of enabled generation targets for the orchestrator.
 * @param generateCli Gets whether CLI generation is enabled for the orchestrator.
 */
public record PipelineOrchestratorModel(
        String serviceName,
        String servicePackage,
        Set<GenerationTarget> enabledTargets,
        boolean generateCli
) {
    /**
     * Creates a new PipelineOrchestratorModel with the supplied service identity and generation configuration.
     *
     * @param serviceName the orchestrator service name; must not be null
     * @param servicePackage the orchestrator service package; must not be null
     * @param enabledTargets the set of enabled generation targets; must not be null
     * @param generateCli whether CLI generation is enabled for the orchestrator
     * @throws IllegalArgumentException if any parameter documented as 'must not be null' is null
     */
    public PipelineOrchestratorModel {
        if (serviceName == null)
            throw new IllegalArgumentException("serviceName cannot be null");
        if (servicePackage == null)
            throw new IllegalArgumentException("servicePackage cannot be null");
        if (enabledTargets == null)
            throw new IllegalArgumentException("enabledTargets cannot be null");
    }
}