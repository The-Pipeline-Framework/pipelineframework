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
     * Resolve generation targets for each pipeline step and update the compilation context.
     *
     * Determines targets using the context's transport mode (defaults to GRPC when null), updates each step model's enabledTargets, replaces the context's step models with the updated list, and stores the union of all enabledTargets as the context's resolved targets.
     *
     * @param ctx the pipeline compilation context whose step models and transport mode are read and whose step models and resolved targets are updated
     * @throws Exception if an error occurs during target resolution or while updating the context
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
     * Determine which generation targets apply for a deployment role under a transport mode.
     *
     * @param role the deployment role to resolve targets for
     * @param transportMode the transport mode that influences target selection
     * @return the set of GenerationTarget values applicable to the given role and transport mode
     * @throws IllegalArgumentException if the deployment role is not supported
     */
    private Set<GenerationTarget> resolveTargetsForRole(
            DeploymentRole role, TransportMode transportMode) {
        TargetResolutionStrategy strategy = strategiesByRole.get(role);
        if (strategy == null) {
            throw new IllegalArgumentException("Unsupported deployment role: " + role);
        }
        return strategy.resolve(transportMode);
    }

    /**
     * Determine the set of generation targets applicable to a pipeline step model.
     *
     * If the model delegates to an external service, the step emits only the local-client target;
     * otherwise targets are resolved from the model's deployment role and the provided transport mode.
     *
     * @param model the pipeline step model to resolve targets for
     * @param transportMode the transport mode used to influence resolution (may be null if caller applies a default)
     * @return a set of GenerationTarget values applicable to the given step model
     */
    private Set<GenerationTarget> resolveTargetsForModel(PipelineStepModel model, TransportMode transportMode) {
        if (model.delegateService() != null) {
            // Delegated steps only resolve local client target here.
            // External adapter generation is bound later in PipelineBindingConstructionPhase.
            return Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
        }
        if (transportMode == TransportMode.LOCAL
            && model.sideEffect()
            && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
            return Set.of(GenerationTarget.LOCAL_CLIENT_STEP);
        }
        return resolveTargetsForRole(model.deploymentRole(), transportMode);
    }
}
