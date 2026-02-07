package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.AspectExpansionProcessor;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ResolvedStep;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Extracts semantic models from annotated elements.
 * This phase discovers and extracts PipelineStepModel instances from @PipelineStep annotated classes.
 */
public class ModelExtractionPhase implements PipelineCompilationPhase {

    /**
     * Creates a new ModelExtractionPhase.
     */
    public ModelExtractionPhase() {
    }

    @Override
    public String name() {
        return "Model Extraction Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Extract pipeline step models
        List<PipelineStepModel> stepModels = extractStepModels(ctx);

        // Add template-derived models for orchestrator/plugin host modules
        List<PipelineStepModel> templateModels = extractTemplateModels(ctx);
        if (!templateModels.isEmpty()) {
            stepModels.addAll(templateModels);
        }
        ctx.setStepModels(stepModels);
    }

    private List<PipelineStepModel> extractStepModels(PipelineCompilationContext ctx) {
        Set<? extends Element> pipelineStepElements =
            ctx.getRoundEnv().getElementsAnnotatedWith(PipelineStep.class);

        List<PipelineStepModel> stepModels = new ArrayList<>();
        PipelineStepIRExtractor irExtractor = new PipelineStepIRExtractor(ctx.getProcessingEnv());

        for (Element annotatedElement : pipelineStepElements) {
            if (annotatedElement.getKind() != ElementKind.CLASS) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "@PipelineStep can only be applied to classes", annotatedElement);
                continue;
            }

            TypeElement serviceClass = (TypeElement) annotatedElement;

            // Extract semantic information into model
            var result = irExtractor.extract(serviceClass);
            if (result == null) {
                continue;
            }

            // For now, just add the extracted model
            // Transport target resolution will happen in a later phase
            stepModels.add(result.model());
        }

        return stepModels;
    }

    /**
     * Create pipeline step models derived from the configured pipeline template, assigning deployment roles
     * according to the compilation context and available aspect/plugin implementations.
     *
     * The method returns an empty list when there is no template configuration, no template steps, or when
     * template-derived steps are not applicable given the current orchestrator/plugin-host/runtime mapping.
     * When applicable, it builds models from the template, expands them with aspects that provide plugin
     * implementations, and assigns DeploymentRole.ORCHESTRATOR_CLIENT to client-facing models and
     * DeploymentRole.PLUGIN_SERVER to side-effect models where plugins are required or colocated.
     *
     * @param ctx the compilation context providing the pipeline template, aspect models, and runtime mapping
     * @return the list of pipeline step models derived from the template; empty if none apply
     */
    private List<PipelineStepModel> extractTemplateModels(PipelineCompilationContext ctx) {
        PipelineTemplateConfig config = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig cfg
            ? cfg
            : null;
        if (config == null || config.steps() == null || config.steps().isEmpty()) {
            return List.of();
        }

        boolean hasOrchestrator = ctx.getRoundEnv() != null
            && !ctx.getRoundEnv().getElementsAnnotatedWith(org.pipelineframework.annotation.PipelineOrchestrator.class).isEmpty();
        if (!ctx.isPluginHost() && !hasOrchestrator) {
            return List.of();
        }

        List<PipelineStepModel> baseModels = buildTemplateStepModels(config);
        if (baseModels.isEmpty()) {
            return List.of();
        }

        boolean colocatedPlugins = ctx.isTransportModeLocal() || isMonolithLayout(ctx);
        if (ctx.isPluginHost() && !colocatedPlugins) {
            Set<String> pluginAspectNames = PluginBindingBuilder.extractPluginAspectNames(ctx);
            if (pluginAspectNames.isEmpty()) {
                return List.of();
            }
            List<org.pipelineframework.processor.ir.PipelineAspectModel> filteredAspects = ctx.getAspectModels().stream()
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

        List<org.pipelineframework.processor.ir.PipelineAspectModel> expandableAspects = ctx.getAspectModels().stream()
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

    /**
     * Create PipelineStepModel instances from the provided pipeline template configuration.
     *
     * Builds a model for each valid PipelineTemplateStep using generated service class names,
     * package segments, input/output type mappings, resolved streaming shape, default execution
     * mode, and a deployment role of PIPELINE_SERVER. Steps with a missing or blank base package,
     * null entries, or invalid formatted names are skipped.
     *
     * @param config the pipeline template configuration containing the base package and step definitions
     * @return a list of PipelineStepModel derived from the template steps; empty if no models could be produced
     */
    private List<PipelineStepModel> buildTemplateStepModels(PipelineTemplateConfig config) {
        String basePackage = config.basePackage();
        if (basePackage == null || basePackage.isBlank()) {
            return List.of();
        }

        List<PipelineStepModel> models = new ArrayList<>();
        for (PipelineTemplateStep step : config.steps()) {
            if (step == null) {
                continue;
            }

            String stepName = step.name();
            String formattedName = NamingPolicy.formatForClassName(NamingPolicy.stripProcessPrefix(stepName));
            if (formattedName == null || formattedName.isBlank()) {
                continue;
            }

            String serviceName = "Process" + formattedName + "Service";
            String serviceNameForPackage = toPackageSegment(stepName);
            String servicePackage = basePackage + "." + serviceNameForPackage + ".service";

            String inputType = step.inputTypeName();
            String outputType = step.outputTypeName();
            TypeMapping inputMapping = buildMapping(basePackage, inputType);
            TypeMapping outputMapping = buildMapping(basePackage, outputType);

            PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName(serviceName)
                .servicePackage(servicePackage)
                .serviceClassName(ClassName.get(servicePackage, serviceName))
                .inputMapping(inputMapping)
                .outputMapping(outputMapping)
                .streamingShape(StreamingShapeResolver.streamingShape(step.cardinality()))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .build();

            models.add(model);
        }

        return models;
    }

    private TypeMapping buildMapping(String basePackage, String typeName) {
        if (typeName == null || typeName.isBlank()) {
            return new TypeMapping(null, null, false);
        }
        ClassName domainType = ClassName.get(basePackage + ".common.domain", typeName);
        ClassName mapperType = ClassName.get(basePackage + ".common.mapper", typeName + "Mapper");
        return new TypeMapping(domainType, mapperType, true);
    }

    private List<PipelineStepModel> expandAspects(
        List<PipelineStepModel> baseModels,
        List<org.pipelineframework.processor.ir.PipelineAspectModel> aspects
    ) {
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

    /**
     * Create a copy of a PipelineStepModel with the specified deployment role.
     *
     * @param model the original step model to copy
     * @param role the deployment role to assign on the returned model
     * @return a new PipelineStepModel identical to {@code model} except with its deployment role set to {@code role}
     */
    private PipelineStepModel withDeploymentRole(PipelineStepModel model, DeploymentRole role) {
        return new PipelineStepModel(
            model.serviceName(),
            model.generatedName(),
            model.servicePackage(),
            model.serviceClassName(),
            model.inputMapping(),
            model.outputMapping(),
            model.streamingShape(),
            model.enabledTargets(),
            model.executionMode(),
            role,
            model.sideEffect(),
            model.cacheKeyGenerator()
        );
    }

    /**
     * Determine whether the configured runtime layout is MONOLITH.
     *
     * @param ctx the compilation context from which the runtime mapping is obtained
     * @return `true` if a runtime mapping exists and its layout is `MONOLITH`, `false` otherwise
     */
    private boolean isMonolithLayout(PipelineCompilationContext ctx) {
        PipelineRuntimeMapping mapping = ctx.getRuntimeMapping();
        return mapping != null && mapping.layout() == PipelineRuntimeMapping.Layout.MONOLITH;
    }

    /**
     * Checks whether an aspect model specifies a plugin implementation class in its configuration.
     *
     * @param aspect the aspect model to inspect; may be null
     * @return `true` if the aspect's config contains a non-blank `"pluginImplementationClass"` entry, `false` otherwise
     */
    private boolean hasPluginImplementation(org.pipelineframework.processor.ir.PipelineAspectModel aspect) {
        if (aspect == null || aspect.config() == null) {
            return false;
        }
        Object value = aspect.config().get("pluginImplementationClass");
        return value != null && !value.toString().isBlank();
    }

    private String toPackageSegment(String name) {
        if (name == null || name.isBlank()) {
            return "service";
        }
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        String sanitized = normalized.replaceAll("[^a-z0-9]+", "_");
        if (sanitized.startsWith("_")) {
            sanitized = sanitized.substring(1);
        }
        if (sanitized.endsWith("_")) {
            sanitized = sanitized.substring(0, sanitized.length() - 1);
        }
        return sanitized.isBlank() ? "service" : sanitized;
    }
}