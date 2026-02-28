package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Objects;
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
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.MapperFallbackMode;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Extracts semantic models from annotated elements.
 * This phase discovers and extracts PipelineStepModel instances from @PipelineStep annotated classes.
 */
public class ModelExtractionPhase implements PipelineCompilationPhase {
    public static final String NO_YAML_DEFINITIONS_MESSAGE =
        "No YAML step definitions were found. Falling back to annotation-driven extraction.";
    private static final String MAPPER_FALLBACK_GLOBAL_OPTION = "pipeline.mapper.fallback.enabled";

    private final ModelContextRoleEnricher contextRoleEnricher;

    /**
     * Creates a new ModelExtractionPhase.
     */
    public ModelExtractionPhase() {
        this(new ModelContextRoleEnricher());
    }

    ModelExtractionPhase(ModelContextRoleEnricher contextRoleEnricher) {
        this.contextRoleEnricher = Objects.requireNonNull(contextRoleEnricher, "contextRoleEnricher");
    }

    /**
     * Human-readable name of this compilation phase.
     *
     * @return the phase name "Model Extraction Phase"
     */
    @Override
    public String name() {
        return "Model Extraction Phase";
    }

    /**
     * Orchestrates extraction and contextual enrichment of pipeline step models from the compilation context.
     *
     * Extracts step models from YAML step definitions, applies context roles and aspects when YAML definitions are present,
     * logs informational notes when enrichment produces no changes or when no YAML definitions are found, and stores the
     * final list of PipelineStepModel instances into the provided context.
     *
     * @param ctx the pipeline compilation context containing step definitions, processing environment, and storage for results
     * @throws Exception if an error occurs during model extraction or contextual enrichment
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        List<org.pipelineframework.processor.ir.StepDefinition> stepDefinitions = ctx.getStepDefinitions();
        boolean hasYamlStepDefinitions = stepDefinitions != null && !stepDefinitions.isEmpty();

        List<PipelineStepModel> stepModels;
        if (hasYamlStepDefinitions) {
            // Extract pipeline step models based on explicit YAML step definitions.
            stepModels = new ArrayList<>(extractStepModelsFromYaml(ctx, stepDefinitions));
            List<PipelineStepModel> contextualModels = contextRoleEnricher.enrich(ctx, stepModels);
            if (contextualModels != null && !contextualModels.isEmpty()) {
                stepModels = contextualModels;
            } else if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.NOTE,
                    "Contextual role/aspect enrichment produced no additional models; preserving YAML-derived models.");
            }
        } else {
            if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.NOTE,
                    NO_YAML_DEFINITIONS_MESSAGE);
            }
            // Preserve backward compatibility for legacy template pipelines that do not declare
            // explicit service/operator step definitions.
            stepModels = new ArrayList<>(extractStepModelsFromAnnotations(ctx));
            List<PipelineStepModel> contextualModels = contextRoleEnricher.enrich(ctx, stepModels);
            if (contextualModels != null && !contextualModels.isEmpty()) {
                stepModels = contextualModels;
            }
        }
        ctx.setStepModels(deduplicateByServiceName(stepModels));
    }

    /**
     * Extract pipeline step models based on YAML configuration (StepDefinition objects).
     * This is the new YAML-driven approach for generating step models.
     *
     * @param ctx the compilation context containing the parsed StepDefinition objects
     * @return list of PipelineStepModel objects based on YAML configuration
     */
    private List<PipelineStepModel> extractStepModelsFromYaml(
            PipelineCompilationContext ctx,
            List<org.pipelineframework.processor.ir.StepDefinition> stepDefinitions) {
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
     * Fallback extraction path for annotation-driven internal steps.
     *
     * @param ctx the compilation context containing the current round environment
     * @return list of extracted step models from @PipelineStep-annotated classes
     */
    private List<PipelineStepModel> extractStepModelsFromAnnotations(PipelineCompilationContext ctx) {
        if (ctx.getRoundEnv() == null || ctx.getProcessingEnv() == null) {
            return List.of();
        }

        List<PipelineStepModel> stepModels = new ArrayList<>();
        PipelineStepIRExtractor irExtractor = new PipelineStepIRExtractor(ctx.getProcessingEnv());
        Set<? extends Element> annotatedElements = ctx.getRoundEnv().getElementsAnnotatedWith(PipelineStep.class);
        for (Element element : annotatedElements) {
            if (element instanceof TypeElement serviceClass) {
                var result = irExtractor.extract(serviceClass);
                if (result != null) {
                    stepModels.add(result.model());
                }
            }
        }
        return stepModels;
    }

    /**
     * Builds a PipelineStepModel that represents the provided StepDefinition.
     *
     * For INTERNAL steps, verifies the referenced service class exists and is annotated with
     * @PipelineStep, extracts semantic information from the class, and applies the YAML-derived
     * identity. For DELEGATED steps, constructs a delegated model via createDelegatedStepModel.
     *
     * @param ctx the compilation context
     * @param stepDef the step definition to convert
     * @param irExtractor the extractor used to obtain semantic information from annotated classes
     * @return a PipelineStepModel populated from the step definition, or `null` if validation or extraction fails
     * @throws IllegalStateException if the step kind is unknown
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
        };
    }

    /**
     * Build a PipelineStepModel representing a delegated step described by the given StepDefinition.
     *
     * Creates a model configured to invoke an existing delegate service (setting delegateService,
     * externalMapper when applicable, type mappings, generation targets, and defaults for execution
     * and deployment). Returns null when validation fails (delegate class not found, delegate does not
     * implement a supported reactive service interface, input/output types cannot be resolved, or an
     * explicit or inferred external mapper cannot be resolved or is incompatible).
     *
     * @param ctx compilation context providing processing utilities and configuration
     * @param stepDef YAML-derived step definition describing the delegated step
     * @return a configured PipelineStepModel for the delegated step, or `null` if the step could not be validated or resolved
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
                    + "ReactiveStreamingClientService, "
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
        boolean fallbackGloballyEnabled = isMapperFallbackGloballyEnabled(ctx);
        boolean fallbackRequested = stepDef.mapperFallback() == MapperFallbackMode.JACKSON;
        boolean typesDiffer = !inputType.equals(reactiveSignature.inputType())
            || !outputType.equals(reactiveSignature.outputType());
        MapperFallbackMode effectiveFallback = MapperFallbackMode.NONE;

        if (fallbackRequested && fallbackGloballyEnabled && externalMapper == null && typesDiffer) {
            effectiveFallback = MapperFallbackMode.JACKSON;
        }

        if (stepDef.externalMapper() != null && externalMapper == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Skipping delegated step '" + stepDef.name()
                    + "': operator mapper '" + stepDef.externalMapper().canonicalName()
                    + "' was specified but could not be resolved.");
            return null;
        }
        if (stepDef.externalMapper() == null
            && externalMapper == null
            && typesDiffer
            && effectiveFallback == MapperFallbackMode.NONE) {
            String fallbackMessage = fallbackRequested && !fallbackGloballyEnabled
                ? " Mapper fallback was requested but global option '" + MAPPER_FALLBACK_GLOBAL_OPTION + "' is disabled."
                : "";
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Skipping delegated step '" + stepDef.name()
                    + "': no operator mapper provided and YAML types ["
                    + inputType + " -> " + outputType
                    + "] do not match delegate types ["
                    + reactiveSignature.inputType() + " -> " + reactiveSignature.outputType() + "]."
                    + fallbackMessage);
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
        TypeMapping inputMapping = new TypeMapping(inputType, null, false, reactiveSignature.inputType());
        TypeMapping outputMapping = new TypeMapping(outputType, null, false, reactiveSignature.outputType());

        // Derive package from execution class or use default
        String servicePackage = stepDef.executionClass().packageName().isEmpty()
            ? "org.pipelineframework.pipeline"
            : stepDef.executionClass().packageName() + ".pipeline";

        String serviceName = toYamlServiceName(stepDef.name());

        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(stepDef.executionClass())
            .inputMapping(inputMapping)
            .outputMapping(outputMapping)
            .streamingShape(reactiveSignature.shape())
            .enabledTargets(targets)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .delegateService(stepDef.executionClass())
            .externalMapper(externalMapper)
            .mapperFallbackMode(effectiveFallback)
            .build();
    }

    private boolean isMapperFallbackGloballyEnabled(PipelineCompilationContext ctx) {
        if (ctx == null || ctx.getProcessingEnv() == null || ctx.getProcessingEnv().getOptions() == null) {
            return false;
        }
        String configured = ctx.getProcessingEnv().getOptions().get(MAPPER_FALLBACK_GLOBAL_OPTION);
        return configured != null && Boolean.parseBoolean(configured.trim());
    }

    /**
     * Locate or infer an ExternalMapper implementation suitable for the delegated step's
     * application and operator input/output types.
     *
     * @param ctx the compilation context used to resolve type elements and report diagnostics
     * @param stepDef the YAML step definition that may specify an explicit external mapper
     * @param applicationInputType the application's expected input type for the step
     * @param applicationOutputType the application's expected output type for the step
     * @param operatorInputType the delegate/operator's input type
     * @param operatorOutputType the delegate/operator's output type
     * @return the ClassName of a compatible ExternalMapper, or `null` if no mapper is required,
     *         none could be resolved, or validation failed
     */
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
                    "Operator mapper class '" + stepDef.externalMapper().canonicalName()
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

    /**
     * Infers a single ExternalMapper implementation matching the specified application/operator input and output types for a step.
     *
     * @param ctx the compilation context used to access the round environment and emit diagnostics
     * @param stepName the YAML step name (used for diagnostics)
     * @param applicationInputType the application's input type to match
     * @param operatorInputType the operator's input type to match
     * @param applicationOutputType the application's output type to match
     * @param operatorOutputType the operator's output type to match
     * @return the ClassName of the matching ExternalMapper, or `null` if no unique compatible implementation can be inferred
     */
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
                "Step '" + stepName + "' requires an operator mapper, but no source candidates were available for inference.");
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
                "Step '" + stepName + "' requires an operator mapper for types ["
                    + applicationInputType + " -> " + operatorInputType + ", "
                    + operatorOutputType + " -> " + applicationOutputType
                    + "], but no matching ExternalMapper implementation was found. "
                    + "The mapper may be in a dependency JAR or produced in a different processing round. "
                    + "Ensure it is compiled in this round or specify it explicitly in YAML.");
            return null;
        }

        if (matchingCandidates.size() > 1) {
            String candidates = matchingCandidates.stream()
                .map(ClassName::canonicalName)
                .sorted()
                .collect(Collectors.joining(", "));
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Step '" + stepName + "' has ambiguous operator mapper inference. Matching candidates: " + candidates);
            return null;
        }

        return matchingCandidates.getFirst();
    }

    /**
     * Validates that the given mapper element's generic type parameters match the specified
     * application and operator input/output types, reporting diagnostics on mismatch.
     *
     * @param ctx the pipeline compilation context used to report diagnostics
     * @param stepName the YAML step name used for diagnostic messages
     * @param mapperElement the candidate mapper class element to validate
     * @param applicationInputType the expected application input type
     * @param operatorInputType the expected operator input type
     * @param applicationOutputType the expected application output type
     * @param operatorOutputType the expected operator output type
     * @return `true` if `mapperElement` declares type parameters that exactly match the four
     *         provided types, `false` otherwise
     */
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

    /**
     * Checks whether a candidate ExternalMapper's generic type parameters match the expected application and operator input/output types for the given step.
     *
     * @param ctx the pipeline compilation context used for type utilities and reporting
     * @param stepName the name of the step (used in diagnostic messages)
     * @param mapperElement the TypeElement of the candidate mapper class
     * @param applicationInputType the expected application-level input type
     * @param operatorInputType the expected operator-level input type
     * @param applicationOutputType the expected application-level output type
     * @param operatorOutputType the expected operator-level output type
     * @param reportErrors if true, emit compilation errors when the mapper is not a valid ExternalMapper or its type parameters do not match
     * @return `true` if the mapper implements org.pipelineframework.mapper.ExternalMapper with type arguments equal to
     *         applicationInputType, operatorInputType, applicationOutputType, operatorOutputType; `false` otherwise.
     */
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
                    "Operator mapper '" + mapperElement.getQualifiedName()
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
                "Operator mapper '" + mapperElement.getQualifiedName() + "' has incompatible type parameters for step '"
                    + stepName + "'. Expected ExternalMapper<"
                    + applicationInputType + ", " + operatorInputType + ", "
                    + applicationOutputType + ", " + operatorOutputType + ">.");
        }

        return matches;
    }

    /**
     * Create a new PipelineStepModel that preserves all attributes of the extracted model
     * but replaces its service identity with the name derived from the provided YAML StepDefinition.
     *
     * @param stepDef the YAML step definition whose name is used to derive the service identity
     * @param extractedModel the model extracted from the annotated/internal class whose fields are copied
     * @return a PipelineStepModel with `serviceName` and `generatedName` set from the YAML definition and all other properties copied from `extractedModel`
     */
    private PipelineStepModel applyYamlIdentityToInternalModel(
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            PipelineStepModel extractedModel) {
        String serviceName = toYamlServiceName(stepDef.name());
        return extractedModel.toBuilder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .build();
    }

    /**
     * Convert a YAML step name into a Java service class name for the pipeline.
     *
     * @param stepName the original step name from YAML (may include a process prefix)
     * @return the generated service class name (for example, "ProcessXyzService"); returns "ProcessStepService" when the formatted name is blank
     */
    private String toYamlServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String formatted = NamingPolicy.formatForClassName(NamingPolicy.stripProcessPrefix(stepName));
        if (formatted == null || formatted.isBlank()) {
            return "ProcessStepService";
        }
        return "Process" + formatted + "Service";
    }

    /**
     * Locates which reactive service interface the delegate implements and derives its reactive signature.
     *
     * If the delegate implements exactly one reactive service interface, returns a ReactiveSignature
     * describing the streaming shape and input/output types. If the delegate implements none, returns
     * `null`. If the delegate implements more than one reactive interface, reports an ERROR via the
     * provided messager and returns `null`.
     *
     * @param delegateElement the delegate class to inspect for implemented reactive service interfaces
     * @return the resolved ReactiveSignature when exactly one reactive interface is implemented, `null` otherwise
     */
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

    /**
     * If the given delegateElement implements the specified reactive interface, adds a corresponding
     * ReactiveSignature (constructed with the provided shape) to `matches` and appends the
     * interface name to `matchNames`.
     *
     * @param matches       list to which a found ReactiveSignature will be appended
     * @param matchNames    list to which the matched interface's qualified name will be appended
     * @param typeUtils     utility for type operations
     * @param delegateElement element to inspect for the reactive interface
     * @param reactiveInterface fully qualified name of the reactive interface to search for
     * @param shape         streaming shape to use when constructing the ReactiveSignature
     */
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

    /**
     * Construct a ReactiveSignature from a declared reactive type by extracting its first two type arguments.
     *
     * @param shape the streaming shape to assign to the signature
     * @param reactiveType a declared reactive interface type whose first two type arguments represent input and output; may be null
     * @return a ReactiveSignature with the given shape and the first two type arguments as input and output types, or `null` if {@code reactiveType} is null or has fewer than two type arguments
     */
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

    /**
     * Locate a declared supertype with the given qualified name by recursively scanning the provided type's supertypes.
     *
     * @param type the starting type to inspect
     * @param targetQualifiedName the fully qualified name of the target supertype to find
     * @return the matching {@link DeclaredType} whose element's qualified name equals {@code targetQualifiedName}, or {@code null} if no match is found
     */
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

    private List<PipelineStepModel> deduplicateByServiceName(List<PipelineStepModel> stepModels) {
        Map<String, PipelineStepModel> uniqueByServiceName = new LinkedHashMap<>();
        for (PipelineStepModel model : stepModels) {
            // Keep first occurrence so concrete @PipelineStep models take precedence over template fallbacks.
            uniqueByServiceName.putIfAbsent(model.serviceName(), model);
        }
        return new ArrayList<>(uniqueByServiceName.values());
    }
}
