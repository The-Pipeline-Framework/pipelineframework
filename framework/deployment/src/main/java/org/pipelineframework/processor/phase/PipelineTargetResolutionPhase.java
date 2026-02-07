package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TransportMode;

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

    /**
     * Resolves generation targets for each pipeline step according to the context's transport mode and deployment role, updates each step model with those targets, and stores the aggregated resolved targets back into the compilation context.
     *
     * The transport mode is taken from the context and defaults to GRPC when not present.
     *
     * @param ctx the pipeline compilation context whose step models and transport mode are read and whose step models and resolved targets are replaced with the computed results
     * @throws Exception if target resolution or context update fails
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        TransportMode transportMode = ctx.getTransportMode() == null ? TransportMode.GRPC : ctx.getTransportMode();

        // Apply transport targets and resolve client/server roles for each step model
        List<PipelineStepModel> updatedModels = new ArrayList<>();
        for (PipelineStepModel model : ctx.getStepModels()) {
            Set<GenerationTarget> targets = resolveTargetsForRole(model.deploymentRole(), transportMode);
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

    /**
     * Resolve the set of generation targets for a given deployment role and transport mode.
     *
     * <p>Client roles return the client-side generation target corresponding to the transport
     * (REST -> REST_CLIENT_STEP, LOCAL -> LOCAL_CLIENT_STEP, GRPC -> CLIENT_STEP). Non-client
     * roles return the resource/service target for the transport (REST -> REST_RESOURCE,
     * GRPC -> GRPC_SERVICE). Note: for non-client roles with transportMode == LOCAL this method
     * returns GRPC_SERVICE to drive side-effect bean generation.
     *
     * @param role the deployment role to resolve targets for
     * @param transportMode the transport mode that influences which targets are selected
     * @return a set of GenerationTarget values applicable to the given role and transport mode
     */
    private Set<GenerationTarget> resolveTargetsForRole(
            DeploymentRole role, TransportMode transportMode) {
        boolean clientRole = role == DeploymentRole.ORCHESTRATOR_CLIENT || role == DeploymentRole.PLUGIN_CLIENT;
        if (clientRole) {
            return switch (transportMode) {
                case REST -> Set.of(GenerationTarget.REST_CLIENT_STEP);
                case LOCAL -> Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
                case GRPC -> Set.of(GenerationTarget.CLIENT_STEP);
            };
        }
        return switch (transportMode) {
            case REST -> Set.of(GenerationTarget.REST_RESOURCE);
            // LOCAL keeps GRPC_SERVICE here only to drive side-effect bean generation.
            // PipelineGenerationPhase intentionally skips full gRPC adapter emission when transportMode == LOCAL.
            case LOCAL, GRPC -> Set.of(GenerationTarget.GRPC_SERVICE);
        };
    }
}