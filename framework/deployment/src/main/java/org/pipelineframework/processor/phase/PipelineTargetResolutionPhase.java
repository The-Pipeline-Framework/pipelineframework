package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    private final EnumMap<DeploymentRole, TargetResolutionStrategy> strategiesByRole;

    /**
     * Creates a new PipelineTargetResolutionPhase.
     */
    public PipelineTargetResolutionPhase() {
        this(new ClientRoleTargetResolutionStrategy(), new ServerRoleTargetResolutionStrategy());
    }

    /**
     * Creates a new PipelineTargetResolutionPhase with explicit strategies.
     */
    public PipelineTargetResolutionPhase(
            TargetResolutionStrategy clientRoleStrategy,
            TargetResolutionStrategy serverRoleStrategy) {
        Objects.requireNonNull(clientRoleStrategy, "clientRoleStrategy must not be null");
        Objects.requireNonNull(serverRoleStrategy, "serverRoleStrategy must not be null");
        this.strategiesByRole = new EnumMap<>(DeploymentRole.class);
        this.strategiesByRole.put(DeploymentRole.ORCHESTRATOR_CLIENT, clientRoleStrategy);
        this.strategiesByRole.put(DeploymentRole.PLUGIN_CLIENT, clientRoleStrategy);
        this.strategiesByRole.put(DeploymentRole.PIPELINE_SERVER, serverRoleStrategy);
        this.strategiesByRole.put(DeploymentRole.PLUGIN_SERVER, serverRoleStrategy);
        this.strategiesByRole.put(DeploymentRole.REST_SERVER, serverRoleStrategy);
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
        TransportMode mode = ctx.getTransportMode();
        TransportMode transportMode = Objects.requireNonNullElse(mode, TransportMode.GRPC);

        // Apply transport targets and resolve client/server roles for each step model
        List<PipelineStepModel> updatedModels = new ArrayList<>();
        for (PipelineStepModel model : ctx.getStepModels()) {
            Set<GenerationTarget> targets = resolveTargetsForModel(model, transportMode);
            PipelineStepModel updatedModel = model.toBuilder()
                .enabledTargets(targets)
                .build();
            updatedModels.add(updatedModel);
        }
        ctx.setStepModels(updatedModels);

        // Set the resolved targets in the context
        Set<GenerationTarget> resolvedTargets = updatedModels.stream()
            .flatMap(model -> model.enabledTargets().stream())
            .collect(Collectors.toSet());
        ctx.setResolvedTargets(resolvedTargets);
    }

    /**
     * Resolve the set of generation targets for a given deployment role and transport mode.
     *
     * <p>Client roles return the client-side generation target corresponding to the transport
     * (REST -> REST_CLIENT_STEP, LOCAL -> LOCAL_CLIENT_STEP, GRPC -> CLIENT_STEP). Non-client
     * roles return the resource/service target for the transport (REST -> REST_RESOURCE,
     * GRPC -> GRPC_SERVICE). For non-client roles with transportMode == LOCAL this method returns
     * GRPC_SERVICE_SIDE_EFFECT_ONLY to drive side-effect bean generation without full gRPC adapter emission.
     *
     * @param role the deployment role to resolve targets for
     * @param transportMode the transport mode that influences which targets are selected
     * @return a set of GenerationTarget values applicable to the given role and transport mode
     */
    private Set<GenerationTarget> resolveTargetsForRole(
            DeploymentRole role, TransportMode transportMode) {
        TargetResolutionStrategy strategy = strategiesByRole.get(role);
        if (strategy == null) {
            throw new IllegalArgumentException("Unsupported deployment role: " + role);
        }
        return strategy.resolve(transportMode);
    }

    private Set<GenerationTarget> resolveTargetsForModel(PipelineStepModel model, TransportMode transportMode) {
        if (model.delegateService() != null) {
            // Delegated steps should always emit a local client contract plus the external adapter.
            return Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
        }
        return resolveTargetsForRole(model.deploymentRole(), transportMode);
    }
}
