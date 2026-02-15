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
 * Orchestrates the expansion of template-derived models with aspects, plugin filtering,
 * and deployment role assignment. Extracted from ModelExtractionPhase.
 */
class TemplateExpansionOrchestrator {

    private static final String PLUGIN_IMPLEMENTATION_CLASS = "pluginImplementationClass";

    /**
     * Expand template models based on the compilation context: colocated vs remote plugins,
     * plugin filtering, role assignment, and aspect expansion.
     *
     * @param ctx the compilation context providing aspect models, runtime mapping, and plugin state
     * @param baseModels the base template-derived step models
     * @return the expanded list of step models with appropriate roles assigned
     */
    List<PipelineStepModel> expandTemplateModels(PipelineCompilationContext ctx, List<PipelineStepModel> baseModels) {
        if (baseModels.isEmpty()) {
            return List.of();
        }

        boolean hasOrchestrator = ctx.getRoundEnv() != null
            && !ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class).isEmpty();
        if (!ctx.isPluginHost() && !hasOrchestrator) {
            return List.of();
        }

        boolean colocatedPlugins = ctx.isTransportModeLocal() || isMonolithLayout(ctx);
        if (ctx.isPluginHost() && !colocatedPlugins) {
            return expandRemotePluginModels(ctx, baseModels);
        }

        List<PipelineAspectModel> expandableAspects = ctx.getAspectModels().stream()
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

    private List<PipelineStepModel> expandRemotePluginModels(
            PipelineCompilationContext ctx, List<PipelineStepModel> baseModels) {
        Set<String> pluginAspectNames = PluginBindingBuilder.extractPluginAspectNames(ctx);
        if (pluginAspectNames.isEmpty()) {
            return List.of();
        }
        List<PipelineAspectModel> filteredAspects = ctx.getAspectModels().stream()
            .filter(aspect -> pluginAspectNames.contains(aspect.name()))
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
        return model.withDeploymentRole(role);
    }

    private boolean isMonolithLayout(PipelineCompilationContext ctx) {
        PipelineRuntimeMapping mapping = ctx.getRuntimeMapping();
        return mapping != null && mapping.layout() == PipelineRuntimeMapping.Layout.MONOLITH;
    }

    private boolean hasPluginImplementation(PipelineAspectModel aspect) {
        if (aspect == null || aspect.config() == null) {
            return false;
        }
        Object value = aspect.config().get(PLUGIN_IMPLEMENTATION_CLASS);
        return value != null && !value.toString().isBlank();
    }
}
