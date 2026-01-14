package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Resolves generation targets and client/server roles based on configuration and annotation settings.
 * This phase determines which targets (gRPC, REST, client, server) should be generated
 * and decides client/server roles for each step.
 */
public class PipelineTargetResolutionPhase implements PipelineCompilationPhase {

    /**
     * Creates a new PipelineTargetResolutionPhase.
     */
    public PipelineTargetResolutionPhase() {
    }

    @Override
    public String name() {
        return "Pipeline Target Resolution Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        boolean isGrpc = ctx.isTransportModeGrpc();

        // Apply transport targets and resolve client/server roles for each step model
        List<PipelineStepModel> updatedModels = new ArrayList<>();
        for (PipelineStepModel model : ctx.getStepModels()) {
            Set<GenerationTarget> targets = resolveTargetsForRole(model.deploymentRole(), isGrpc);
            PipelineStepModel updatedModel = new PipelineStepModel(
                model.serviceName(),
                model.generatedName(),
                model.servicePackage(),
                model.serviceClassName(),
                model.inputMapping(),
                model.outputMapping(),
                model.streamingShape(),
                targets,
                model.executionMode(),
                model.deploymentRole(),
                model.sideEffect(),
                model.cacheKeyGenerator());
            updatedModels.add(updatedModel);
        }
        ctx.setStepModels(updatedModels);

        // Set the resolved targets in the context
        Set<GenerationTarget> resolvedTargets = updatedModels.stream()
            .flatMap(model -> model.enabledTargets().stream())
            .collect(java.util.stream.Collectors.toSet());
        ctx.setResolvedTargets(resolvedTargets);
    }

    private Set<GenerationTarget> resolveTargetsForRole(DeploymentRole role, boolean isGrpc) {
        boolean clientRole = role == DeploymentRole.ORCHESTRATOR_CLIENT || role == DeploymentRole.PLUGIN_CLIENT;
        if (isGrpc) {
            return clientRole
                ? Set.of(GenerationTarget.CLIENT_STEP)
                : Set.of(GenerationTarget.GRPC_SERVICE);
        }
        return clientRole
            ? Set.of(GenerationTarget.REST_CLIENT_STEP)
            : Set.of(GenerationTarget.REST_RESOURCE);
    }
}
