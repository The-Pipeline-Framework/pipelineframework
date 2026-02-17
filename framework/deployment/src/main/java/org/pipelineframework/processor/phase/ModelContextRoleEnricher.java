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

    private boolean isMonolithLayout(PipelineCompilationContext ctx) {
        PipelineRuntimeMapping mapping = ctx.getRuntimeMapping();
        return mapping != null && mapping.layout() == PipelineRuntimeMapping.Layout.MONOLITH;
    }

    private boolean hasPluginImplementation(PipelineAspectModel aspect) {
        if (aspect == null || aspect.config() == null) {
            return false;
        }
        Object value = aspect.config().get("pluginImplementationClass");
        return value != null && !value.toString().isBlank();
    }
}
