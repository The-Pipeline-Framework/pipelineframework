package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.processor.AspectExpansionProcessor;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ResolvedStep;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Applies context-specific deployment roles and aspect expansion to extracted step models.
 */
class ModelContextRoleEnricher {

    List<PipelineStepModel> enrich(PipelineCompilationContext ctx, List<PipelineStepModel> baseModels) {
        if (baseModels == null || baseModels.isEmpty()) {
            return List.of();
        }

        boolean hasOrchestrator = ctx.getRoundEnv() != null
            && !ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class).isEmpty();
        if (isRuntimeMappedStepModule(ctx, hasOrchestrator)) {
            return handleRuntimeMappedStepModule(baseModels);
        }
        if (!ctx.isPluginHost() && !hasOrchestrator) {
            return List.of();
        }

        boolean colocatedPlugins = ctx.isTransportModeLocal() || isMonolithLayout(ctx);
        if (ctx.isPluginHost() && !colocatedPlugins) {
            return handlePluginHostDistributed(ctx, baseModels);
        }

        return handleOrchestratorAndColocated(ctx, baseModels, colocatedPlugins);
    }

    private List<PipelineStepModel> handlePluginHostDistributed(
            PipelineCompilationContext ctx,
            List<PipelineStepModel> baseModels) {
        Set<String> pluginAspectNames = PluginBindingBuilder.extractPluginAspectNames(ctx);
        if (pluginAspectNames == null || pluginAspectNames.isEmpty()) {
            return List.of();
        }

        List<PipelineAspectModel> filteredAspects = ctx.getAspectModels().stream()
            .filter(aspect -> aspect != null && pluginAspectNames.contains(aspect.name()))
            .filter(this::hasPluginImplementation)
            .toList();
        if (filteredAspects.isEmpty()) {
            return List.of();
        }

        List<PipelineStepModel> expanded = expandAspects(baseModels, filteredAspects);
        return expanded.stream()
            .filter(PipelineStepModel::sideEffect)
            .map(model -> withDeploymentRole(model, DeploymentRole.PLUGIN_SERVER))
            .toList();
    }

    private List<PipelineStepModel> handleOrchestratorAndColocated(
            PipelineCompilationContext ctx,
            List<PipelineStepModel> baseModels,
            boolean colocatedPlugins) {
        List<PipelineAspectModel> expandableAspects = ctx.getAspectModels().stream()
            .filter(aspect -> aspect != null)
            .filter(this::hasPluginImplementation)
            .toList();
        List<PipelineStepModel> expanded = expandAspects(baseModels, expandableAspects);
        List<PipelineStepModel> clientModels = expanded.stream()
            .map(model -> withDeploymentRole(model, DeploymentRole.ORCHESTRATOR_CLIENT))
            .toList();
        if (!colocatedPlugins) {
            return clientModels;
        }
        List<PipelineStepModel> pluginModels = expanded.stream()
            .filter(PipelineStepModel::sideEffect)
            .map(model -> withDeploymentRole(model, DeploymentRole.PLUGIN_SERVER))
            .toList();
        if (pluginModels.isEmpty()) {
            return clientModels;
        }
        List<PipelineStepModel> combined = new ArrayList<>(pluginModels.size() + clientModels.size());
        combined.addAll(pluginModels);
        combined.addAll(clientModels);
        return combined;
    }

    private List<PipelineStepModel> expandAspects(
            List<PipelineStepModel> baseModels,
            List<PipelineAspectModel> aspects) {
        if (aspects == null || aspects.isEmpty()) {
            return baseModels;
        }

        List<ResolvedStep> resolvedSteps = baseModels.stream()
            .map(model -> new ResolvedStep(model, null, null))
            .toList();

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(resolvedSteps, aspects);
        return expanded.stream()
            .map(ResolvedStep::model)
            .toList();
    }

    private PipelineStepModel withDeploymentRole(PipelineStepModel model, DeploymentRole role) {
        return model.toBuilder()
            .deploymentRole(role)
            .build();
    }

    private List<PipelineStepModel> handleRuntimeMappedStepModule(List<PipelineStepModel> baseModels) {
        return baseModels.stream()
            .filter(this::isServerCandidate)
            .map(this::ensureServerRole)
            .toList();
    }

    private PipelineStepModel ensureServerRole(PipelineStepModel model) {
        DeploymentRole role = model.deploymentRole();
        if (role == null) {
            return withDeploymentRole(model, DeploymentRole.PIPELINE_SERVER);
        }
        if (role == DeploymentRole.ORCHESTRATOR_CLIENT || role == DeploymentRole.PLUGIN_CLIENT) {
            throw new IllegalStateException(
                "Runtime-mapped step module cannot retain client deployment role for step " + model.serviceName());
        }
        return model;
    }

    private boolean isMonolithLayout(PipelineCompilationContext ctx) {
        PipelineRuntimeMapping mapping = ctx.getRuntimeMapping();
        return mapping != null && mapping.layout() == PipelineRuntimeMapping.Layout.MONOLITH;
    }

    private boolean isRuntimeMappedStepModule(PipelineCompilationContext ctx, boolean hasOrchestrator) {
        PipelineRuntimeMapping mapping = ctx.getRuntimeMapping();
        return mapping != null
            && mapping.layout() == PipelineRuntimeMapping.Layout.MODULAR
            && ctx.getModuleName() != null
            && !ctx.getModuleName().isBlank()
            && !ctx.isPluginHost()
            && !hasOrchestrator;
    }

    private boolean isServerCandidate(PipelineStepModel model) {
        DeploymentRole role = model.deploymentRole();
        // A null role means deployment enrichment has not assigned the step yet, so it is still
        // eligible for server-side generation unless it later becomes an orchestrator or plugin client.
        return role == null || (role != DeploymentRole.ORCHESTRATOR_CLIENT && role != DeploymentRole.PLUGIN_CLIENT);
    }

    private boolean hasPluginImplementation(PipelineAspectModel aspect) {
        if (aspect == null || aspect.config() == null) {
            return false;
        }
        Object value = aspect.config().get("pluginImplementationClass");
        return value != null && !value.toString().isBlank();
    }
}
