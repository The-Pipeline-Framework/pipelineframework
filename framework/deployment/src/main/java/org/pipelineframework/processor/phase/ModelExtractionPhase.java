package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.AspectExpansionProcessor;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ResolvedStep;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
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
        // Extract pipeline step models based on YAML configuration
        List<PipelineStepModel> stepModels = new ArrayList<>(extractStepModelsFromYaml(ctx));

        // Only use legacy template-derived model synthesis when no YAML step definitions were parsed.
        List<org.pipelineframework.processor.ir.StepDefinition> stepDefinitions = ctx.getStepDefinitions();
        boolean hasYamlStepDefinitions = stepDefinitions != null && !stepDefinitions.isEmpty();
        if (hasYamlStepDefinitions) {
            List<PipelineStepModel> contextualModels = applyContextRolesAndAspects(ctx, stepModels);
            if (contextualModels != null && !contextualModels.isEmpty()) {
                stepModels = contextualModels;
            }
        } else {
            if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.NOTE,
                    "No YAML step definitions were found. Skipping step generation (YAML-driven mode).");
            }
        }
        ctx.setStepModels(stepModels);
    }

    /**
     * Extract pipeline step models based on YAML configuration (StepDefinition objects).
     * This is the new YAML-driven approach for generating step models.
     *
     * @param ctx the compilation context containing the parsed StepDefinition objects
     * @return list of PipelineStepModel objects based on YAML configuration
     */
    private List<PipelineStepModel> extractStepModelsFromYaml(PipelineCompilationContext ctx) {
        List<org.pipelineframework.processor.ir.StepDefinition> stepDefinitions = ctx.getStepDefinitions();
        if (stepDefinitions == null || stepDefinitions.isEmpty()) {
            return List.of();
        }

        List<PipelineStepModel> stepModels = new ArrayList<>();
        PipelineStepIRExtractor irExtractor = new PipelineStepIRExtractor(ctx.getProcessingEnv());

        for (org.pipelineframework.processor.ir.StepDefinition stepDef : stepDefinitions) {
            PipelineStepModel stepModel = createStepModelFromDefinition(ctx, stepDef, irExtractor);
            if (stepModel != null) {
                stepModels.add(stepModel);
            }
        }

        return stepModels;
    }

    /**
     * Create a PipelineStepModel from a StepDefinition.
     *
     * @param ctx the compilation context
     * @param stepDef the step definition to convert
     * @param irExtractor the extractor for annotation processing
     * @return a PipelineStepModel based on the StepDefinition
     */
    private PipelineStepModel createStepModelFromDefinition(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            PipelineStepIRExtractor irExtractor) {

        // Determine if this is an internal or delegated step using switch for exhaustiveness
        return switch (stepDef.kind()) {
            case INTERNAL -> {
                // For internal steps, verify that the service class is annotated with @PipelineStep
                TypeElement serviceClass = ctx.getProcessingEnv().getElementUtils()
                    .getTypeElement(stepDef.executionClass().canonicalName());

                if (serviceClass == null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.ERROR,
                        "Internal step service class '" + stepDef.executionClass().canonicalName() +
                        "' not found for step '" + stepDef.name() + "'");
                    yield null;
                }

                if (serviceClass.getAnnotation(PipelineStep.class) == null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.ERROR,
                        "Internal step service class '" + stepDef.executionClass().canonicalName() +
                        "' must be annotated with @PipelineStep for step '" + stepDef.name() + "'");
                    yield null;
                }

                // Extract semantic information from the annotated class
                var result = irExtractor.extract(serviceClass);
                if (result == null) {
                    yield null;
                }

                // Keep semantic details from the annotation, but force YAML-driven step identity.
                yield applyYamlIdentityToInternalModel(stepDef, result.model());
            }
            case DELEGATED -> {
                // For delegated steps, create a model based on the delegate service
                yield createDelegatedStepModel(ctx, stepDef);
            }
            default -> {
                // Fail-fast guard for unknown/future StepKind values so missing handling is surfaced immediately.
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "Unknown step kind '" + stepDef.kind() + "' for step '" + stepDef.name() +
                    "'. Valid kinds are: INTERNAL, DELEGATED.");
                throw new IllegalStateException("Unknown step kind: " + stepDef.kind());
            }
        };
    }

    /**
     * Create a PipelineStepModel for a delegated step.
     *
     * @param ctx the compilation context
     * @param stepDef the step definition for the delegated step
     * @return a PipelineStepModel for the delegated step
     */
    private PipelineStepModel createDelegatedStepModel(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef) {

        // Validate that the delegate service exists and implements a reactive service interface
        TypeElement delegateElement = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(stepDef.executionClass().canonicalName());

        if (delegateElement == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Delegate service class '" + stepDef.executionClass().canonicalName() +
                "' not found for step '" + stepDef.name() + "'");
            return null;
        }

        var typeUtils = ctx.getProcessingEnv().getTypeUtils();
        ReactiveSignature reactiveSignature = resolveReactiveSignature(
            delegateElement,
            typeUtils,
            ctx.getProcessingEnv().getMessager());
        if (reactiveSignature == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Delegate service '" + stepDef.executionClass().canonicalName() +
                "' must implement one of: ReactiveService, ReactiveStreamingService, "
                    + "ReactiveStreamingClientService/ReactiveClientStreamingService, "
                    + "ReactiveBidirectionalStreamingService for step '" +
                stepDef.name() + "'");
            return null;
        }

        TypeName inputType = stepDef.inputType() != null ? stepDef.inputType() : reactiveSignature.inputType();
        TypeName outputType = stepDef.outputType() != null ? stepDef.outputType() : reactiveSignature.outputType();
        if (inputType == null || outputType == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Could not resolve input/output types for delegated step '" + stepDef.name()
                    + "'. Provide 'input'/'output' in YAML or use a parameterized reactive delegate type.");
            return null;
        }

        ClassName externalMapper = resolveDelegatedExternalMapper(
            ctx,
            stepDef,
            inputType,
            outputType,
            reactiveSignature.inputType(),
            reactiveSignature.outputType());
        if (stepDef.externalMapper() != null && externalMapper == null) {
            return null;
        }
        if (stepDef.externalMapper() == null
            && externalMapper == null
            && (!inputType.equals(reactiveSignature.inputType())
                || !outputType.equals(reactiveSignature.outputType()))) {
            return null;
        }

        // Create the model with the appropriate configuration for delegation
        Set<GenerationTarget> targets = EnumSet.of(GenerationTarget.CLIENT_STEP);
        if (ctx.isTransportModeRest()) {
            targets.add(GenerationTarget.REST_CLIENT_STEP);
        }
        if (ctx.isTransportModeLocal()) {
            targets.add(GenerationTarget.LOCAL_CLIENT_STEP);
        }

        // Create type mappings based on the input/output types specified in the YAML
        TypeMapping inputMapping = new TypeMapping(inputType, null, false);
        TypeMapping outputMapping = new TypeMapping(outputType, null, false);

        // Derive package from execution class or use default
        String servicePackage = stepDef.executionClass().packageName().isEmpty()
            ? "org.pipelineframework.pipeline"
            : stepDef.executionClass().packageName() + ".pipeline";

        String serviceName = toYamlServiceName(stepDef.name());

        return new PipelineStepModel(
            serviceName,
            serviceName,
            servicePackage,
            stepDef.executionClass(),
            inputMapping,
            outputMapping,
            reactiveSignature.shape(),
            targets,
            ExecutionMode.DEFAULT,
            DeploymentRole.PIPELINE_SERVER,
            false,
            null,
            OrderingRequirement.RELAXED,
            ThreadSafety.SAFE,
            stepDef.executionClass(),
            externalMapper
        );
    }

    private ClassName resolveDelegatedExternalMapper(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            TypeName applicationInputType,
            TypeName applicationOutputType,
            TypeName operatorInputType,
            TypeName operatorOutputType) {
        if (stepDef.externalMapper() != null) {
            TypeElement mapperElement = ctx.getProcessingEnv().getElementUtils()
                .getTypeElement(stepDef.externalMapper().canonicalName());
            if (mapperElement == null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "External mapper class '" + stepDef.externalMapper().canonicalName()
                        + "' not found for step '" + stepDef.name() + "'");
                return null;
            }
            if (!isCompatibleExternalMapper(
                ctx,
                stepDef.name(),
                mapperElement,
                applicationInputType,
                operatorInputType,
                applicationOutputType,
                operatorOutputType)) {
                return null;
            }
            return ClassName.get(mapperElement);
        }

        if (applicationInputType.equals(operatorInputType) && applicationOutputType.equals(operatorOutputType)) {
            return null;
        }

        return inferExternalMapper(
            ctx,
            stepDef.name(),
            applicationInputType,
            operatorInputType,
            applicationOutputType,
            operatorOutputType);
    }

    private ClassName inferExternalMapper(
            PipelineCompilationContext ctx,
            String stepName,
            TypeName applicationInputType,
            TypeName operatorInputType,
            TypeName applicationOutputType,
            TypeName operatorOutputType) {
        if (ctx.getRoundEnv() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Step '" + stepName + "' requires an external mapper, but no source candidates were available for inference.");
            return null;
        }

        List<ClassName> matchingCandidates = new ArrayList<>();
        for (Element rootElement : ctx.getRoundEnv().getRootElements()) {
            if (rootElement.getKind() != ElementKind.CLASS) {
                continue;
            }
            if (!(rootElement instanceof TypeElement candidateElement)) {
                continue;
            }
            if (isCompatibleExternalMapper(
                ctx,
                stepName,
                candidateElement,
                applicationInputType,
                operatorInputType,
                applicationOutputType,
                operatorOutputType,
                false)) {
                matchingCandidates.add(ClassName.get(candidateElement));
            }
        }

        if (matchingCandidates.isEmpty()) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Step '" + stepName + "' requires an external mapper for types ["
                    + applicationInputType + " -> " + operatorInputType + ", "
                    + operatorOutputType + " -> " + applicationOutputType
                    + "], but no matching ExternalMapper implementation was found.");
            return null;
        }

        if (matchingCandidates.size() > 1) {
            String candidates = matchingCandidates.stream()
                .map(ClassName::canonicalName)
                .sorted()
                .collect(Collectors.joining(", "));
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Step '" + stepName + "' has ambiguous external mapper inference. Matching candidates: " + candidates);
            return null;
        }

        return matchingCandidates.getFirst();
    }

    private boolean isCompatibleExternalMapper(
            PipelineCompilationContext ctx,
            String stepName,
            TypeElement mapperElement,
            TypeName applicationInputType,
            TypeName operatorInputType,
            TypeName applicationOutputType,
            TypeName operatorOutputType) {
        return isCompatibleExternalMapper(
            ctx,
            stepName,
            mapperElement,
            applicationInputType,
            operatorInputType,
            applicationOutputType,
            operatorOutputType,
            true);
    }

    private boolean isCompatibleExternalMapper(
            PipelineCompilationContext ctx,
            String stepName,
            TypeElement mapperElement,
            TypeName applicationInputType,
            TypeName operatorInputType,
            TypeName applicationOutputType,
            TypeName operatorOutputType,
            boolean reportErrors) {
        Types typeUtils = ctx.getProcessingEnv().getTypeUtils();
        DeclaredType externalMapperType = findReactiveSupertype(
            typeUtils,
            mapperElement.asType(),
            "org.pipelineframework.mapper.ExternalMapper");

        if (externalMapperType == null || externalMapperType.getTypeArguments().size() != 4) {
            if (reportErrors) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "External mapper '" + mapperElement.getQualifiedName()
                        + "' must implement org.pipelineframework.mapper.ExternalMapper<IApp, ILib, OApp, OLib>"
                        + " for step '" + stepName + "'");
            }
            return false;
        }

        TypeName candidateApplicationInput = TypeName.get(externalMapperType.getTypeArguments().get(0));
        TypeName candidateOperatorInput = TypeName.get(externalMapperType.getTypeArguments().get(1));
        TypeName candidateApplicationOutput = TypeName.get(externalMapperType.getTypeArguments().get(2));
        TypeName candidateOperatorOutput = TypeName.get(externalMapperType.getTypeArguments().get(3));

        boolean matches = candidateApplicationInput.equals(applicationInputType)
            && candidateOperatorInput.equals(operatorInputType)
            && candidateApplicationOutput.equals(applicationOutputType)
            && candidateOperatorOutput.equals(operatorOutputType);

        if (!matches && reportErrors) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "External mapper '" + mapperElement.getQualifiedName() + "' has incompatible type parameters for step '"
                    + stepName + "'. Expected ExternalMapper<"
                    + applicationInputType + ", " + operatorInputType + ", "
                    + applicationOutputType + ", " + operatorOutputType + ">.");
        }

        return matches;
    }

    private PipelineStepModel applyYamlIdentityToInternalModel(
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            PipelineStepModel extractedModel) {
        String serviceName = toYamlServiceName(stepDef.name());
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(extractedModel.servicePackage())
            .serviceClassName(extractedModel.serviceClassName())
            .inputMapping(extractedModel.inputMapping())
            .outputMapping(extractedModel.outputMapping())
            .streamingShape(extractedModel.streamingShape())
            .enabledTargets(extractedModel.enabledTargets())
            .executionMode(extractedModel.executionMode())
            .deploymentRole(extractedModel.deploymentRole())
            .sideEffect(extractedModel.sideEffect())
            .cacheKeyGenerator(extractedModel.cacheKeyGenerator())
            .orderingRequirement(extractedModel.orderingRequirement())
            .threadSafety(extractedModel.threadSafety())
            .delegateService(extractedModel.delegateService())
            .externalMapper(extractedModel.externalMapper())
            .build();
    }

    private String toYamlServiceName(String stepName) {
        String formatted = NamingPolicy.formatForClassName(NamingPolicy.stripProcessPrefix(stepName));
        if (formatted == null || formatted.isBlank()) {
            return "ProcessStepService";
        }
        return "Process" + formatted + "Service";
    }

    private ReactiveSignature resolveReactiveSignature(
            TypeElement delegateElement,
            Types typeUtils,
            javax.annotation.processing.Messager messager) {
        List<ReactiveSignature> matches = new ArrayList<>();
        List<String> matchNames = new ArrayList<>();

        collectReactiveMatch(matches, matchNames, typeUtils, delegateElement,
            "org.pipelineframework.service.ReactiveService", StreamingShape.UNARY_UNARY);
        collectReactiveMatch(matches, matchNames, typeUtils, delegateElement,
            "org.pipelineframework.service.ReactiveStreamingService", StreamingShape.UNARY_STREAMING);
        collectReactiveMatch(matches, matchNames, typeUtils, delegateElement,
            "org.pipelineframework.service.ReactiveStreamingClientService", StreamingShape.STREAMING_UNARY);
        collectReactiveMatch(matches, matchNames, typeUtils, delegateElement,
            "org.pipelineframework.service.ReactiveClientStreamingService", StreamingShape.STREAMING_UNARY);
        collectReactiveMatch(matches, matchNames, typeUtils, delegateElement,
            "org.pipelineframework.service.ReactiveBidirectionalStreamingService", StreamingShape.STREAMING_STREAMING);

        if (matches.size() > 1) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Delegate service '" + delegateElement.getQualifiedName()
                    + "' implements multiple reactive service interfaces: " + String.join(", ", matchNames)
                    + ". Please implement exactly one.");
            return null;
        }
        return matches.isEmpty() ? null : matches.get(0);
    }

    private void collectReactiveMatch(
            List<ReactiveSignature> matches,
            List<String> matchNames,
            Types typeUtils,
            TypeElement delegateElement,
            String reactiveInterface,
            StreamingShape shape) {
        DeclaredType type = findReactiveSupertype(typeUtils, delegateElement.asType(), reactiveInterface);
        if (type == null) {
            return;
        }
        ReactiveSignature signature = reactiveSignature(shape, type);
        if (signature != null) {
            matches.add(signature);
            matchNames.add(reactiveInterface);
        }
    }

    private ReactiveSignature reactiveSignature(StreamingShape shape, DeclaredType reactiveType) {
        if (reactiveType == null || reactiveType.getTypeArguments().size() < 2) {
            return null;
        }
        return new ReactiveSignature(
            shape,
            TypeName.get(reactiveType.getTypeArguments().get(0)),
            TypeName.get(reactiveType.getTypeArguments().get(1))
        );
    }

    private DeclaredType findReactiveSupertype(Types types, TypeMirror type, String targetQualifiedName) {
        if (type == null || type.getKind() == TypeKind.NONE) {
            return null;
        }
        if (type.getKind() == TypeKind.DECLARED) {
            DeclaredType declaredType = (DeclaredType) type;
            if (declaredType.asElement() instanceof TypeElement te
                && targetQualifiedName.contentEquals(te.getQualifiedName())) {
                return declaredType;
            }
        }

        for (TypeMirror supertype : types.directSupertypes(type)) {
            DeclaredType match = findReactiveSupertype(types, supertype, targetQualifiedName);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private record ReactiveSignature(StreamingShape shape, TypeName inputType, TypeName outputType) {
    }

    private List<PipelineStepModel> applyContextRolesAndAspects(
            PipelineCompilationContext ctx,
            List<PipelineStepModel> baseModels) {
        if (baseModels == null || baseModels.isEmpty()) {
            return List.of();
        }
        boolean hasOrchestrator = ctx.getRoundEnv() != null
            && !ctx.getRoundEnv().getElementsAnnotatedWith(org.pipelineframework.annotation.PipelineOrchestrator.class).isEmpty();
        if (!ctx.isPluginHost() && !hasOrchestrator) {
            return List.of();
        }

        boolean colocatedPlugins = ctx.isTransportModeLocal() || isMonolithLayout(ctx);
        if (ctx.isPluginHost() && !colocatedPlugins) {
            Set<String> pluginAspectNames = PluginBindingBuilder.extractPluginAspectNames(ctx);
            if (pluginAspectNames == null || pluginAspectNames.isEmpty()) {
                return List.of();
            }
            List<org.pipelineframework.processor.ir.PipelineAspectModel> filteredAspects = ctx.getAspectModels().stream()
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

        List<org.pipelineframework.processor.ir.PipelineAspectModel> expandableAspects = ctx.getAspectModels().stream()
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
        return new PipelineStepModel.Builder()
            .serviceName(model.serviceName())
            .generatedName(model.generatedName())
            .servicePackage(model.servicePackage())
            .serviceClassName(model.serviceClassName())
            .inputMapping(model.inputMapping())
            .outputMapping(model.outputMapping())
            .streamingShape(model.streamingShape())
            .enabledTargets(model.enabledTargets())
            .executionMode(model.executionMode())
            .deploymentRole(role)
            .sideEffect(model.sideEffect())
            .cacheKeyGenerator(model.cacheKeyGenerator())
            .orderingRequirement(model.orderingRequirement())
            .threadSafety(model.threadSafety())
            .delegateService(model.delegateService())
            .externalMapper(model.externalMapper())
            .build();
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

}
