package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
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
import org.pipelineframework.processor.ir.ReactiveReturnKind;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

/**
 * Extracts semantic models from YAML step definitions and legacy {@code @PipelineStep} annotations.
 */
public class ModelExtractionPhase implements PipelineCompilationPhase {
    public static final String NO_YAML_DEFINITIONS_MESSAGE =
        "No YAML step definitions were found. Falling back to annotation-driven extraction.";
    private static final String MAPPER_FALLBACK_GLOBAL_OPTION = "pipeline.mapper.fallback.enabled";
    private static final String DEFAULT_SERVICE_PACKAGE = "org.pipelineframework.pipeline.service";

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
        Consumer<String> ctxWarningLogger = message -> {
            if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING, message);
            }
        };
        List<org.pipelineframework.processor.ir.StepDefinition> stepDefinitions = ctx.getStepDefinitions();
        boolean hasYamlStepDefinitions = stepDefinitions != null && !stepDefinitions.isEmpty();

        List<PipelineStepModel> stepModels;
        if (hasYamlStepDefinitions) {
            // Extract pipeline step models based on explicit YAML step definitions.
            stepModels = new ArrayList<>(extractStepModelsFromYaml(ctx, stepDefinitions, ctxWarningLogger));
            // Some template-driven YAMLs declare logical steps without resolvable execution classes
            // for plugin-host modules. Keep legacy behavior by falling back to annotation extraction.
            if (stepModels.isEmpty()) {
                stepModels = new ArrayList<>(extractStepModelsFromAnnotations(ctx));
            }
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
            List<org.pipelineframework.processor.ir.StepDefinition> stepDefinitions,
            Consumer<String> ctxWarningLogger) {
        if (stepDefinitions == null || stepDefinitions.isEmpty()) {
            return List.of();
        }

        List<PipelineStepModel> stepModels = new ArrayList<>();
        PipelineStepIRExtractor irExtractor = new PipelineStepIRExtractor(ctx.getProcessingEnv());

        for (org.pipelineframework.processor.ir.StepDefinition stepDef : stepDefinitions) {
            PipelineStepModel stepModel = createStepModelFromDefinition(ctx, stepDef, irExtractor, ctxWarningLogger);
            if (stepModel != null) {
                stepModels.add(stepModel);
            }
        }

        return stepModels;
    }

    /**
     * Fallback extraction path for legacy annotation-driven internal steps.
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
     * For INTERNAL steps, verifies the referenced service class exists, extracts the implemented
     * service contract, and applies YAML-derived identity and mappings. {@code @PipelineStep}
     * metadata remains a compatibility source when present, but YAML is authoritative.
     * For DELEGATED steps, constructs a delegated model via createDelegatedStepModel.
     * For REMOTE steps, constructs a generated remote-adapter model from template contract metadata.
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
            PipelineStepIRExtractor irExtractor,
            Consumer<String> ctxWarningLogger) {

        // Determine if this is an internal or delegated step using switch for exhaustiveness
        return switch (stepDef.kind()) {
            case INTERNAL -> {
                yield createInternalStepModel(ctx, stepDef, irExtractor);
            }
            case DELEGATED -> {
                // For delegated steps, create a model based on the delegate service
                yield createDelegatedStepModel(ctx, stepDef);
            }
            case REMOTE -> {
                yield createRemoteStepModel(ctx, stepDef, ctxWarningLogger);
            }
            case AWAIT -> {
                yield createAwaitStepModel(ctx, stepDef, ctxWarningLogger);
            }
            case COMMAND -> {
                yield createCommandStepModel(ctx, stepDef, ctxWarningLogger);
            }
            case QUERY -> {
                yield createQueryStepModel(ctx, stepDef, ctxWarningLogger);
            }
        };
    }

    private PipelineStepModel createCommandStepModel(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            Consumer<String> ctxWarningLogger) {
        if (stepDef.inputType() == null || stepDef.outputType() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Command step '" + stepDef.name() + "' must resolve both input and output domain types");
            return null;
        }
        StreamingShape streamingShape = stepDef.streamingShapeHint() != null
            ? stepDef.streamingShapeHint()
            : StreamingShape.UNARY_UNARY;
        if (streamingShape != StreamingShape.UNARY_UNARY) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Command step '" + stepDef.name() + "' supports only ONE_TO_ONE cardinality in v1");
            return null;
        }

        String templateBasePackage = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config
            ? config.basePackage()
            : null;
        TypeName inputType = normalizeLegacyDomainType(stepDef.inputType(), null, templateBasePackage, ctx);
        TypeName outputType = normalizeLegacyDomainType(stepDef.outputType(), null, templateBasePackage, ctx);

        String serviceName = toYamlServiceName(stepDef.name());
        String servicePackage = deriveYamlServicePackage(inputType, ctxWarningLogger);
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(ClassName.get("org.pipelineframework.command", "CommandStepDescriptor"))
            .inputMapping(new TypeMapping(inputType, null, false, inputType))
            .outputMapping(new TypeMapping(outputType, null, false, outputType))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(java.util.Set.of(GenerationTarget.COMMAND_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .cacheKeyGenerator(stepDef.commandIdGenerator())
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private PipelineStepModel createQueryStepModel(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            Consumer<String> ctxWarningLogger) {
        if (stepDef.inputType() == null || stepDef.outputType() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Query step '" + stepDef.name() + "' must resolve both input and output domain types");
            return null;
        }
        StreamingShape streamingShape = stepDef.streamingShapeHint() != null
            ? stepDef.streamingShapeHint()
            : StreamingShape.UNARY_UNARY;
        if (streamingShape != StreamingShape.UNARY_UNARY) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Query step '" + stepDef.name() + "' supports only ONE_TO_ONE cardinality in v1");
            return null;
        }

        String templateBasePackage = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config
            ? config.basePackage()
            : null;
        TypeName inputType = normalizeLegacyDomainType(stepDef.inputType(), null, templateBasePackage, ctx);
        TypeName outputType = normalizeLegacyDomainType(stepDef.outputType(), null, templateBasePackage, ctx);

        String serviceName = toYamlServiceName(stepDef.name());
        String servicePackage = deriveYamlServicePackage(inputType, ctxWarningLogger);
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(ClassName.get("org.pipelineframework.query", "QueryStepDescriptor"))
            .inputMapping(new TypeMapping(inputType, null, false, inputType))
            .outputMapping(new TypeMapping(outputType, null, false, outputType))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(java.util.Set.of(GenerationTarget.QUERY_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private PipelineStepModel createAwaitStepModel(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            Consumer<String> ctxWarningLogger) {
        if (stepDef.inputType() == null || stepDef.outputType() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Await step '" + stepDef.name() + "' must resolve both input and output domain types");
            return null;
        }
        StreamingShape streamingShape = stepDef.streamingShapeHint() != null
            ? stepDef.streamingShapeHint()
            : StreamingShape.UNARY_UNARY;

        String templateBasePackage = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config
            ? config.basePackage()
            : null;
        TypeName inputType = normalizeLegacyDomainType(stepDef.inputType(), null, templateBasePackage, ctx);
        TypeName outputType = normalizeLegacyDomainType(stepDef.outputType(), null, templateBasePackage, ctx);

        String serviceName = toYamlServiceName(stepDef.name());
        String servicePackage = deriveYamlServicePackage(inputType, ctxWarningLogger);
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .inputMapping(new TypeMapping(inputType, null, false, inputType))
            .outputMapping(new TypeMapping(outputType, null, false, outputType))
            .streamingShape(streamingShape)
            .enabledTargets(java.util.Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private PipelineStepModel createRemoteStepModel(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            Consumer<String> ctxWarningLogger) {
        if (stepDef.remoteExecution() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Remote step '" + stepDef.name() + "' is missing execution metadata");
            return null;
        }
        if (stepDef.inputType() == null || stepDef.outputType() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Remote step '" + stepDef.name() + "' must resolve both input and output domain types");
            return null;
        }
        StreamingShape streamingShape = stepDef.streamingShapeHint() != null
            ? stepDef.streamingShapeHint()
            : StreamingShape.UNARY_UNARY;
        if (streamingShape != StreamingShape.UNARY_UNARY) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Remote step '" + stepDef.name() + "' currently supports only unary execution");
            return null;
        }

        String serviceName = toYamlServiceName(stepDef.name());
        String servicePackage = deriveYamlServicePackage(stepDef.inputType(), ctxWarningLogger);
        ClassName generatedAdapterType = ClassName.get(
            servicePackage + org.pipelineframework.processor.PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            serviceName + "RemoteOperatorAdapter");

        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(generatedAdapterType)
            // Remote steps carry the same contract type on both sides of TypeMapping inputMapping/outputMapping.
            // Unlike createDelegatedStepModel, domain vs. gRPC/external types are not split here because the
            // protobuf contract is resolved later from descriptors and the remote adapter uses that directly.
            .inputMapping(new TypeMapping(stepDef.inputType(), null, false, stepDef.inputType()))
            .outputMapping(new TypeMapping(stepDef.outputType(), null, false, stepDef.outputType()))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(EnumSet.of(GenerationTarget.REMOTE_OPERATOR_ADAPTER))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .remoteExecution(stepDef.remoteExecution())
            .build();
    }

    private PipelineStepModel createInternalStepModel(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            PipelineStepIRExtractor irExtractor) {
        TypeElement serviceClass = ctx.getProcessingEnv().getElementUtils()
            .getTypeElement(stepDef.executionClass().canonicalName());

        if (serviceClass == null) {
            PipelineStepModel syntheticModel = createCrossModuleInternalModel(stepDef, ctx);
            if (syntheticModel != null) {
                return syntheticModel;
            }
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal step service class '" + stepDef.executionClass().canonicalName() +
                    "' not found for step '" + stepDef.name() + "'");
            return null;
        }

        SupportedServiceSignature serviceSignature = resolveSupportedInternalSignature(
            ctx,
            serviceClass,
            ctx.getProcessingEnv().getTypeUtils(),
            ctx.getProcessingEnv().getMessager());
        if (serviceSignature == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal step service '" + stepDef.executionClass().canonicalName()
                    + "' must implement exactly one supported service interface or declare exactly one public process(In): Uni<Out>"
                    + springPlainMethodHint(ctx) + " method for step '"
                    + stepDef.name() + "'");
            return null;
        }

        PipelineStepModel extractedModel = createYamlInternalBaseModel(ctx, serviceClass, serviceSignature, irExtractor);
        if (extractedModel == null) {
            return null;
        }
        boolean annotationBacked = serviceClass.getAnnotation(PipelineStep.class) != null;

        StreamingShape yamlShape = stepDef.streamingShapeHint();
        if (yamlShape != null && yamlShape != serviceSignature.shape()) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal step '" + stepDef.name() + "' declares cardinality "
                    + yamlShape + " in YAML, but service '" + stepDef.executionClass().canonicalName()
                    + "' implements " + serviceSignature.shape() + " semantics.");
            return null;
        }

        TypeName inputType = resolveInternalDomainType(
            ctx,
            stepDef.name(),
            "input",
            stepDef.inputType(),
            annotationBacked ? extractedModel.inboundDomainType() : null,
            serviceSignature.inputType());
        if (inputType == null) {
            return null;
        }
        TypeName outputType = resolveInternalDomainType(
            ctx,
            stepDef.name(),
            "output",
            stepDef.outputType(),
            annotationBacked ? extractedModel.outboundDomainType() : null,
            serviceSignature.outputType());
        if (outputType == null) {
            return null;
        }

        ClassName inboundMapper = resolveInternalMapper(
            ctx,
            stepDef.name(),
            "inboundMapper",
            stepDef.inboundMapper(),
            annotationBacked && extractedModel.inputMapping() != null ? castToClassName(extractedModel.inputMapping().mapperType()) : null,
            inputType);
        if (inboundMapper == INVALID_CLASS_NAME) {
            return null;
        }
        ClassName outboundMapper = resolveInternalMapper(
            ctx,
            stepDef.name(),
            "outboundMapper",
            stepDef.outboundMapper(),
            annotationBacked && extractedModel.outputMapping() != null ? castToClassName(extractedModel.outputMapping().mapperType()) : null,
            outputType);
        if (outboundMapper == INVALID_CLASS_NAME) {
            return null;
        }

        String serviceName = toYamlServiceName(stepDef.name());
        ExecutionMode resolvedExecutionMode = executionMode(ctx, stepDef, serviceSignature.apiKind());
        return extractedModel.toBuilder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .inputMapping(new TypeMapping(inputType, inboundMapper, inboundMapper != null, inputType))
            .outputMapping(new TypeMapping(outputType, outboundMapper, outboundMapper != null, outputType))
            .streamingShape(serviceSignature.shape())
            .executionMode(resolvedExecutionMode)
            .serviceApiKind(serviceSignature.apiKind())
            .reactiveReturnKind(serviceSignature.reactiveReturnKind())
            .build();
    }

    private PipelineStepModel createYamlInternalBaseModel(
            PipelineCompilationContext ctx,
            TypeElement serviceClass,
            SupportedServiceSignature serviceSignature,
            PipelineStepIRExtractor irExtractor) {
        if (serviceClass.getAnnotation(PipelineStep.class) != null) {
            var extracted = irExtractor.extract(serviceClass);
            return extracted == null ? null : extracted.model();
        }

        String qualifiedServiceName = serviceClass.getQualifiedName().toString();
        ClassName serviceClassName = null;
        try {
            serviceClassName = ClassName.get(serviceClass);
        } catch (Exception e) {
            if (qualifiedServiceName != null && !qualifiedServiceName.isBlank()) {
                serviceClassName = ClassName.bestGuess(qualifiedServiceName);
            }
        }
        if (serviceClassName == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Could not resolve service class name for YAML internal step service '"
                    + qualifiedServiceName + "'");
            return null;
        }

        return new PipelineStepModel.Builder()
            .serviceName(serviceClass.getSimpleName().toString())
            .generatedName(serviceClass.getSimpleName().toString())
            .servicePackage(ctx.getProcessingEnv().getElementUtils().getPackageOf(serviceClass).getQualifiedName().toString())
            .serviceClassName(serviceClassName)
            .inputMapping(new TypeMapping(serviceSignature.inputType(), null, false, serviceSignature.inputType()))
            .outputMapping(new TypeMapping(serviceSignature.outputType(), null, false, serviceSignature.outputType()))
            .streamingShape(serviceSignature.shape())
            .enabledTargets(EnumSet.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .serviceApiKind(serviceSignature.apiKind())
            .reactiveReturnKind(serviceSignature.reactiveReturnKind())
            .build();
    }

    PipelineStepModel createCrossModuleInternalModel(
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            PipelineCompilationContext ctx) {
        if (stepDef.inputType() == null || stepDef.outputType() == null) {
            return null;
        }
        String templateBasePackage = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config
            ? config.basePackage()
            : null;
        TypeName inputType = normalizeLegacyDomainType(stepDef.inputType(), stepDef.executionClass(), templateBasePackage, ctx);
        TypeName outputType = normalizeLegacyDomainType(stepDef.outputType(), stepDef.executionClass(), templateBasePackage, ctx);
        StreamingShape streamingShape = stepDef.streamingShapeHint() != null
            ? stepDef.streamingShapeHint()
            : StreamingShape.UNARY_UNARY;

        Set<GenerationTarget> targets = EnumSet.of(GenerationTarget.CLIENT_STEP);
        if (ctx.isTransportModeRest()) {
            targets.add(GenerationTarget.REST_CLIENT_STEP);
        }
        if (ctx.isTransportModeLocal()) {
            targets.add(GenerationTarget.LOCAL_CLIENT_STEP);
        }

        String servicePackage = stepDef.executionClass().packageName().isEmpty()
            ? "org.pipelineframework.pipeline"
            : stepDef.executionClass().packageName() + ".pipeline";
        String serviceName = toYamlServiceName(stepDef.name());

        if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Internal step '" + stepDef.name() + "' is defined in YAML but service class '"
                    + stepDef.executionClass().canonicalName()
                    + "' is not on this module classpath. Using YAML type/cardinality metadata for cross-module compilation.");
        }

        DeploymentRole crossModuleRole = ctx.isPluginHost()
            ? DeploymentRole.PLUGIN_CLIENT
            : DeploymentRole.ORCHESTRATOR_CLIENT;

        ExecutionMode resolvedExecutionMode = executionMode(ctx, stepDef, null);
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage(servicePackage)
            .serviceClassName(stepDef.executionClass())
            .inputMapping(new TypeMapping(
                inputType,
                stepDef.inboundMapper(),
                stepDef.inboundMapper() != null,
                inputType))
            .outputMapping(new TypeMapping(
                outputType,
                stepDef.outboundMapper(),
                stepDef.outboundMapper() != null,
                outputType))
            .streamingShape(streamingShape)
            .enabledTargets(targets)
            .executionMode(resolvedExecutionMode)
            .deploymentRole(crossModuleRole)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private TypeName normalizeLegacyDomainType(
        TypeName declaredType,
        ClassName executionClass,
        String basePackageHint,
        PipelineCompilationContext ctx
    ) {
        if (!(declaredType instanceof ClassName className) || !className.packageName().isEmpty()) {
            return declaredType;
        }
        TypeName resolvedType;
        if (basePackageHint != null && !basePackageHint.isBlank()) {
            resolvedType = ClassName.bestGuess(basePackageHint + ".common.domain." + className.simpleName());
        } else {
            if (executionClass == null) {
                return declaredType;
            }
            String executionPkg = executionClass.packageName();
            if (executionPkg == null || executionPkg.isBlank()) {
                return declaredType;
            }
            String basePackage = executionPkg.endsWith(".service")
                ? executionPkg.substring(0, executionPkg.length() - ".service".length())
                : executionPkg;
            resolvedType = ClassName.bestGuess(basePackage + ".common.domain." + className.simpleName());
        }
        if (!isResolvable(resolvedType, ctx)) {
            throw new IllegalStateException("Unresolved inferred domain type: " + resolvedType);
        }
        return resolvedType;
    }

    private boolean isResolvable(TypeName type, PipelineCompilationContext ctx) {
        if (!(type instanceof ClassName className) || ctx == null || ctx.getProcessingEnv() == null) {
            return true;
        }
        return ctx.getProcessingEnv().getElementUtils().getTypeElement(className.canonicalName()) != null;
    }

    private String deriveYamlServicePackage(TypeName domainType, Consumer<String> warningLogger) {
        if (domainType instanceof ClassName className) {
            String packageName = className.packageName();
            String suffix = ".common.domain";
            if (packageName.endsWith(suffix)) {
                return packageName.substring(0, packageName.length() - suffix.length()) + ".service";
            }
            if (warningLogger != null) {
                warningLogger.accept("Falling back to default service package '" + DEFAULT_SERVICE_PACKAGE + "' "
                    + "for domain type '" + domainType + "' with package '" + packageName + "'");
            }
            return DEFAULT_SERVICE_PACKAGE;
        }
        if (warningLogger != null) {
            warningLogger.accept("Falling back to default service package '" + DEFAULT_SERVICE_PACKAGE + "' "
                + "for domain type '" + domainType + "'");
        }
        return DEFAULT_SERVICE_PACKAGE;
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

        // Validate that the delegate service exists and exposes a supported step contract.
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
        Optional<SupportedServiceSignature> delegateSignature = resolveSupportedDelegatedSignature(
            ctx,
            delegateElement,
            typeUtils,
            ctx.getProcessingEnv().getMessager(),
            stepDef.delegatedMethodName());
        if (delegateSignature.isEmpty()) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Delegate service '" + stepDef.executionClass().canonicalName() +
                    stepDef.delegatedMethodName().map(method -> "::" + method).orElse("")
                    + "' must expose a supported step contract for step '" + stepDef.name() + "'");
            return null;
        }
        SupportedServiceSignature resolvedDelegateSignature = delegateSignature.get();
        StreamingShape yamlShape = stepDef.streamingShapeHint();
        if (yamlShape != null && yamlShape != resolvedDelegateSignature.shape()) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Delegated step '" + stepDef.name() + "' declares cardinality "
                    + yamlShape + " in YAML, but delegate '" + stepDef.executionClass().canonicalName()
                    + stepDef.delegatedMethodName().map(method -> "::" + method).orElse("")
                    + "' implements " + resolvedDelegateSignature.shape() + " semantics.");
            return null;
        }

        TypeName inputType = stepDef.inputType() != null ? stepDef.inputType() : resolvedDelegateSignature.inputType();
        TypeName outputType = stepDef.outputType() != null ? stepDef.outputType() : resolvedDelegateSignature.outputType();
        if (inputType == null || outputType == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Could not resolve input/output types for delegated step '" + stepDef.name()
                    + "'. Provide 'input'/'output' in YAML or use a parameterized reactive delegate type.");
            return null;
        }

        boolean fallbackGloballyEnabled = isMapperFallbackGloballyEnabled(ctx);
        boolean fallbackRequested = stepDef.mapperFallback() == MapperFallbackMode.JACKSON;
        boolean typesDiffer = !inputType.equals(resolvedDelegateSignature.inputType())
            || !outputType.equals(resolvedDelegateSignature.outputType());
        boolean allowFallbackNoMapper = stepDef.externalMapper() == null
            && fallbackRequested
            && fallbackGloballyEnabled
            && typesDiffer;

        ClassName externalMapper = resolveDelegatedExternalMapper(
            ctx,
            stepDef,
            inputType,
            outputType,
            resolvedDelegateSignature.inputType(),
            resolvedDelegateSignature.outputType(),
            allowFallbackNoMapper);
        MapperFallbackMode effectiveFallback = MapperFallbackMode.NONE;

        if (allowFallbackNoMapper && externalMapper == null) {
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
                    + resolvedDelegateSignature.inputType() + " -> " + resolvedDelegateSignature.outputType() + "]."
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
        TypeMapping inputMapping = new TypeMapping(inputType, null, false, resolvedDelegateSignature.inputType());
        TypeMapping outputMapping = new TypeMapping(outputType, null, false, resolvedDelegateSignature.outputType());

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
            .streamingShape(resolvedDelegateSignature.shape())
            .enabledTargets(targets)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .delegateService(stepDef.executionClass())
            .delegateMethodName(stepDef.delegatedMethodName())
            .externalMapper(externalMapper)
            .mapperFallbackMode(effectiveFallback)
            .serviceApiKind(resolvedDelegateSignature.apiKind())
            .reactiveReturnKind(resolvedDelegateSignature.reactiveReturnKind())
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
            TypeName operatorOutputType,
            boolean allowFallbackNoMapper) {
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
            operatorOutputType,
            !allowFallbackNoMapper);
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
            TypeName operatorOutputType,
            boolean reportMissingCandidateErrorFlag) {
        if (ctx.getRoundEnv() == null) {
            reportMissingCandidateError(ctx, stepName, reportMissingCandidateErrorFlag);
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
            if (reportMissingCandidateErrorFlag) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "Step '" + stepName + "' requires an operator mapper for types ["
                        + applicationInputType + " -> " + operatorInputType + ", "
                        + operatorOutputType + " -> " + applicationOutputType
                        + "], but no matching ExternalMapper implementation was found. "
                        + "The mapper may be in a dependency JAR or produced in a different processing round. "
                        + "Ensure it is compiled in this round or specify it explicitly in YAML.");
            }
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

    private void reportMissingCandidateError(
            PipelineCompilationContext ctx,
            String stepName,
            boolean reportMissingCandidateError) {
        if (!reportMissingCandidateError) {
            return;
        }
        ctx.getProcessingEnv().getMessager().printMessage(
            javax.tools.Diagnostic.Kind.ERROR,
            "Step '" + stepName + "' requires an operator mapper, but no source candidates were available for inference.");
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

    private Optional<SupportedServiceSignature> resolveSupportedDelegatedSignature(
            PipelineCompilationContext ctx,
            TypeElement delegateElement,
            Types typeUtils,
            javax.annotation.processing.Messager messager,
            Optional<String> delegatedMethodName) {
        if (delegatedMethodName.isPresent() && isSpringRendererProfile(ctx)) {
            return resolveSpringDelegatedMethodSignature(ctx, delegateElement, messager, delegatedMethodName.get());
        }
        if (delegatedMethodName.isPresent()) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Class::method delegated operators are currently supported only by the Spring renderer profile.");
            return Optional.empty();
        }
        if (isSpringRendererProfile(ctx)) {
            return Optional.ofNullable(resolveSupportedInternalSignature(ctx, delegateElement, typeUtils, messager));
        }
        ReactiveSignature reactiveSignature = resolveReactiveSignature(delegateElement, typeUtils, messager);
        if (reactiveSignature == null) {
            return Optional.empty();
        }
        return Optional.of(new SupportedServiceSignature(
            ServiceApiKind.REACTIVE,
            reactiveSignature.shape(),
            reactiveSignature.inputType(),
            reactiveSignature.outputType(),
            ReactiveReturnKind.MUTINY_UNI,
            null));
    }

    private Optional<SupportedServiceSignature> resolveSpringDelegatedMethodSignature(
            PipelineCompilationContext ctx,
            TypeElement delegateElement,
            javax.annotation.processing.Messager messager,
            String delegatedMethodName) {
        List<ExecutableElement> publicInstanceMatches = new ArrayList<>();
        boolean staticMatch = false;
        boolean namedMethodExists = false;
        for (Element enclosed : delegateElement.getEnclosedElements()) {
            if (!(enclosed instanceof ExecutableElement method)
                || !delegatedMethodName.contentEquals(method.getSimpleName())) {
                continue;
            }
            namedMethodExists = true;
            if (method.getModifiers().contains(Modifier.STATIC)) {
                staticMatch = true;
                continue;
            }
            if (method.getModifiers().contains(Modifier.PUBLIC)) {
                publicInstanceMatches.add(method);
            }
        }
        if (publicInstanceMatches.isEmpty()) {
            String reason = staticMatch
                ? "is static, but Spring delegated operator methods must be instance bean methods"
                : namedMethodExists
                    ? "is not public"
                    : "was not found";
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Spring delegated operator '" + delegateElement.getQualifiedName() + "::" + delegatedMethodName
                    + "' " + reason + ".");
            return Optional.empty();
        }
        if (publicInstanceMatches.size() > 1) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Spring delegated operator '" + delegateElement.getQualifiedName() + "::" + delegatedMethodName
                    + "' is overloaded. Declare exactly one public instance method with that name.");
            return Optional.empty();
        }

        ExecutableElement method = publicInstanceMatches.getFirst();
        if (method.getParameters().size() != 1) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Spring delegated operator '" + delegateElement.getQualifiedName() + "::" + delegatedMethodName
                    + "' must declare exactly one input parameter.");
            return Optional.empty();
        }
        TypeMirror returnType = method.getReturnType();
        if (returnType.getKind() == TypeKind.VOID) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Spring delegated operator '" + delegateElement.getQualifiedName() + "::" + delegatedMethodName
                    + "' must return an output value.");
            return Optional.empty();
        }

        TypeName inputType = boxIfPrimitive(TypeName.get(method.getParameters().getFirst().asType()));
        if (returnType instanceof DeclaredType declaredReturn && declaredReturn.getTypeArguments().size() == 1) {
            if (isDeclaredType(declaredReturn, "reactor.core.publisher.Mono")) {
                return Optional.of(new SupportedServiceSignature(
                    ServiceApiKind.REACTIVE,
                    StreamingShape.UNARY_UNARY,
                    inputType,
                    boxIfPrimitive(TypeName.get(declaredReturn.getTypeArguments().getFirst())),
                    ReactiveReturnKind.REACTOR_MONO,
                    null));
            }
            if (isDeclaredType(declaredReturn, "java.util.concurrent.CompletionStage")) {
                return Optional.of(new SupportedServiceSignature(
                    ServiceApiKind.REACTIVE,
                    StreamingShape.UNARY_UNARY,
                    inputType,
                    boxIfPrimitive(TypeName.get(declaredReturn.getTypeArguments().getFirst())),
                    ReactiveReturnKind.COMPLETION_STAGE,
                    null));
            }
        }

        if (returnType instanceof DeclaredType declaredReturn && (
            isDeclaredType(declaredReturn, "reactor.core.publisher.Mono")
                || isDeclaredType(declaredReturn, "java.util.concurrent.CompletionStage")
                || isDeclaredType(declaredReturn, "io.smallrye.mutiny.Uni")
                || isDeclaredType(declaredReturn, "io.smallrye.mutiny.Multi")
                || isDeclaredType(declaredReturn, "reactor.core.publisher.Flux"))) {
            String kind = ((TypeElement) declaredReturn.asElement()).getQualifiedName().toString();
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Spring delegated operator '" + delegateElement.getQualifiedName() + "::" + delegatedMethodName
                    + "' does not support return type '" + kind
                    + "'; use Mono<Out>, CompletionStage<Out>, or a blocking return value.");
            return Optional.empty();
        }

        return Optional.of(new SupportedServiceSignature(
            ServiceApiKind.BLOCKING,
            StreamingShape.UNARY_UNARY,
            inputType,
            boxIfPrimitive(TypeName.get(returnType)),
            ReactiveReturnKind.MUTINY_UNI,
            null));
    }

    private SupportedServiceSignature resolveSupportedInternalSignature(
            PipelineCompilationContext ctx,
            TypeElement serviceElement,
            Types typeUtils,
            javax.annotation.processing.Messager messager) {
        List<SupportedServiceSignature> blockingMatches = new ArrayList<>();
        List<String> blockingMatchNames = new ArrayList<>();

        collectSupportedMatch(blockingMatches, blockingMatchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.blocking.BlockingService", ServiceApiKind.BLOCKING, StreamingShape.UNARY_UNARY, null);
        collectSupportedMatch(blockingMatches, blockingMatchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.blocking.BlockingStreamingService", ServiceApiKind.BLOCKING,
            StreamingShape.UNARY_STREAMING,
            "BlockingStreamingService materializes the full output list before downstream emission. "
                + "Use BlockingIteratorService for incremental non-Mutiny 1->N streaming.");
        collectSupportedMatch(blockingMatches, blockingMatchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.blocking.BlockingIteratorService", ServiceApiKind.BLOCKING_ITERATOR,
            StreamingShape.UNARY_STREAMING,
            null);
        collectSupportedMatch(blockingMatches, blockingMatchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.blocking.BlockingStreamingClientService", ServiceApiKind.BLOCKING,
            StreamingShape.STREAMING_UNARY,
            "BlockingStreamingClientService materializes the full input list before invocation. "
                + "Batch retries rerun the full callback.");
        collectSupportedMatch(blockingMatches, blockingMatchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.blocking.BlockingBidirectionalStreamingService", ServiceApiKind.BLOCKING,
            StreamingShape.STREAMING_STREAMING,
            "BlockingBidirectionalStreamingService materializes the full input list and full output list. "
                + "Batch retries rerun the full callback.");

        List<SupportedServiceSignature> matches = new ArrayList<>();
        List<String> matchNames = new ArrayList<>();

        collectSupportedMatch(matches, matchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.ReactiveService", ServiceApiKind.REACTIVE, StreamingShape.UNARY_UNARY, null);
        collectSupportedMatch(matches, matchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.ReactiveStreamingService", ServiceApiKind.REACTIVE, StreamingShape.UNARY_STREAMING, null);
        collectSupportedMatch(matches, matchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.ReactiveStreamingClientService", ServiceApiKind.REACTIVE,
            StreamingShape.STREAMING_UNARY,
            null);
        collectSupportedMatch(matches, matchNames, typeUtils, serviceElement,
            "org.pipelineframework.service.ReactiveBidirectionalStreamingService", ServiceApiKind.REACTIVE,
            StreamingShape.STREAMING_STREAMING,
            null);

        List<String> directSupportedInterfaces = directSupportedInterfaceNames(typeUtils, serviceElement);
        if (directSupportedInterfaces.size() > 1) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal service '" + serviceElement.getQualifiedName()
                    + "' implements multiple supported service interfaces: " + String.join(", ", directSupportedInterfaces)
                    + ". Please implement exactly one.");
            return null;
        }

        List<SupportedServiceSignature> combinedMatches = new ArrayList<>(blockingMatches);
        List<String> combinedMatchNames = new ArrayList<>(blockingMatchNames);
        for (int i = 0; i < matches.size(); i++) {
            SupportedServiceSignature reactiveMatch = matches.get(i);
            boolean impliedByBlocking = blockingMatches.stream()
                .anyMatch(blockingMatch -> blockingMatch.shape() == reactiveMatch.shape());
            if (!impliedByBlocking) {
                combinedMatches.add(reactiveMatch);
                combinedMatchNames.add(matchNames.get(i));
            }
        }

        if (combinedMatches.size() > 1) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal service '" + serviceElement.getQualifiedName()
                    + "' implements multiple supported service interfaces: " + String.join(", ", combinedMatchNames)
                    + ". Please implement exactly one.");
            return null;
        }
        if (combinedMatches.isEmpty()) {
            return resolvePlainUnaryProcessSignature(ctx, serviceElement, messager).orElse(null);
        }
        SupportedServiceSignature match = combinedMatches.get(0);
        if (match.materializingWarning() != null) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                match.materializingWarning(),
                serviceElement);
        }
        return match;
    }

    private Optional<SupportedServiceSignature> resolvePlainUnaryProcessSignature(
            PipelineCompilationContext ctx,
            TypeElement serviceElement,
            javax.annotation.processing.Messager messager) {
        List<SupportedServiceSignature> matches = new ArrayList<>();
        boolean springProfile = isSpringRendererProfile(ctx);
        for (Element enclosed : serviceElement.getEnclosedElements()) {
            if (!(enclosed instanceof ExecutableElement method)) {
                continue;
            }
            if (!method.getModifiers().contains(Modifier.PUBLIC)
                || method.getModifiers().contains(Modifier.STATIC)
                || method.getParameters().size() != 1) {
                continue;
            }
            String methodName = method.getSimpleName().toString();
            TypeMirror returnType = method.getReturnType();
            if ("processBlocking".equals(methodName) && springProfile && returnType.getKind() != TypeKind.VOID) {
                matches.add(new SupportedServiceSignature(
                    ServiceApiKind.BLOCKING,
                    StreamingShape.UNARY_UNARY,
                    TypeName.get(method.getParameters().getFirst().asType()),
                    TypeName.get(returnType),
                    // Blocking services do not have a reactive return. Renderers branch on
                    // ServiceApiKind first; this value is retained only for model compatibility.
                    ReactiveReturnKind.MUTINY_UNI,
                    null));
                continue;
            }
            if (!"process".equals(methodName)
                || !(returnType instanceof DeclaredType declaredReturn)
                || declaredReturn.getTypeArguments().size() != 1) {
                continue;
            }
            ReactiveReturnKind reactiveReturnKind;
            if (isDeclaredType(declaredReturn, "io.smallrye.mutiny.Uni")) {
                reactiveReturnKind = ReactiveReturnKind.MUTINY_UNI;
            } else if (springProfile && isDeclaredType(declaredReturn, "reactor.core.publisher.Mono")) {
                reactiveReturnKind = ReactiveReturnKind.REACTOR_MONO;
            } else {
                continue;
            }
            matches.add(new SupportedServiceSignature(
                ServiceApiKind.REACTIVE,
                StreamingShape.UNARY_UNARY,
                TypeName.get(method.getParameters().getFirst().asType()),
                TypeName.get(declaredReturn.getTypeArguments().getFirst()),
                reactiveReturnKind,
                null));
        }

        if (matches.size() > 1) {
            messager.printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal service '" + serviceElement.getQualifiedName()
                    + "' declares multiple public process(In): Uni<Out>" + springPlainMethodHint(ctx)
                    + " methods. Please declare exactly one.");
            return Optional.empty();
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
    }

    private ExecutionMode executionMode(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.StepDefinition stepDef,
            ServiceApiKind serviceApiKind) {
        ExecutionMode mode = stepDef.runOnVirtualThreads() ? ExecutionMode.VIRTUAL_THREADS : ExecutionMode.DEFAULT;
        if (mode == ExecutionMode.VIRTUAL_THREADS && serviceApiKind == ServiceApiKind.REACTIVE) {
            printVirtualThreadError(
                ctx,
                "Internal step '" + stepDef.name()
                    + "' sets runOnVirtualThreads, but virtual-thread offload is valid only for blocking internal services.");
        } else if (mode == ExecutionMode.VIRTUAL_THREADS && serviceApiKind == null) {
            printVirtualThreadError(
                ctx,
                "Internal step '" + stepDef.name()
                    + "' sets runOnVirtualThreads, but the service class must be available at build time to verify blocking execution.");
        }
        return mode;
    }

    private void printVirtualThreadError(PipelineCompilationContext ctx, String message) {
        if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
            ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR, message);
        }
    }

    private boolean isSpringRendererProfile(PipelineCompilationContext ctx) {
        return ctx != null && "spring".equalsIgnoreCase(ctx.getRendererProfile());
    }

    private String springPlainMethodHint(PipelineCompilationContext ctx) {
        return isSpringRendererProfile(ctx) ? " or process(In): Mono<Out> or processBlocking(In): Out" : "";
    }

    private boolean isDeclaredType(DeclaredType declaredType, String qualifiedName) {
        Element element = declaredType.asElement();
        return element instanceof TypeElement typeElement
            && typeElement.getQualifiedName().contentEquals(qualifiedName);
    }

    private List<String> directSupportedInterfaceNames(Types typeUtils, TypeElement serviceElement) {
        List<String> directNames = new ArrayList<>();
        for (TypeMirror iface : serviceElement.getInterfaces()) {
            Element element = typeUtils.asElement(iface);
            if (element instanceof TypeElement typeElement && isSupportedServiceInterface(typeElement)) {
                directNames.add(typeElement.getQualifiedName().toString());
            }
        }
        return directNames;
    }

    private boolean isSupportedServiceInterface(TypeElement typeElement) {
        String qualifiedName = typeElement.getQualifiedName().toString();
        return qualifiedName.equals("org.pipelineframework.service.blocking.BlockingService")
            || qualifiedName.equals("org.pipelineframework.service.blocking.BlockingStreamingService")
            || qualifiedName.equals("org.pipelineframework.service.blocking.BlockingIteratorService")
            || qualifiedName.equals("org.pipelineframework.service.blocking.BlockingStreamingClientService")
            || qualifiedName.equals("org.pipelineframework.service.blocking.BlockingBidirectionalStreamingService")
            || qualifiedName.equals("org.pipelineframework.service.ReactiveService")
            || qualifiedName.equals("org.pipelineframework.service.ReactiveStreamingService")
            || qualifiedName.equals("org.pipelineframework.service.ReactiveStreamingClientService")
            || qualifiedName.equals("org.pipelineframework.service.ReactiveBidirectionalStreamingService");
    }

    private void collectSupportedMatch(
            List<SupportedServiceSignature> matches,
            List<String> matchNames,
            Types typeUtils,
            TypeElement serviceElement,
            String interfaceName,
            ServiceApiKind apiKind,
            StreamingShape shape,
            String materializingWarning) {
        DeclaredType declared = findReactiveSupertype(typeUtils, serviceElement.asType(), interfaceName);
        if (declared == null || declared.getTypeArguments().size() < 2) {
            return;
        }
        matches.add(new SupportedServiceSignature(
            apiKind,
            shape,
            TypeName.get(declared.getTypeArguments().get(0)),
            TypeName.get(declared.getTypeArguments().get(1)),
            ReactiveReturnKind.MUTINY_UNI,
            materializingWarning));
        matchNames.add(interfaceName);
    }

    private static final ClassName INVALID_CLASS_NAME = ClassName.get("java.lang", "Void");

    private TypeName resolveInternalDomainType(
            PipelineCompilationContext ctx,
            String stepName,
            String direction,
            TypeName yamlType,
            TypeName annotationType,
            TypeName reactiveType) {
        if (yamlType != null) {
            if (reactiveType != null && !yamlType.equals(reactiveType)) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "Internal step '" + stepName + "' declares " + direction + " type '" + yamlType
                        + "' in YAML, but service implementation declares '" + reactiveType + "'.");
                return null;
            }
            if (annotationType != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Internal step '" + stepName + "' declares " + direction + " type '" + yamlType
                        + "' in YAML and deprecated @PipelineStep metadata declares '" + annotationType
                        + "'. YAML is authoritative.");
            }
            return yamlType;
        }
        if (annotationType != null) {
            if (reactiveType != null && !annotationType.equals(reactiveType)) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.ERROR,
                    "Internal step '" + stepName + "' has deprecated @PipelineStep " + direction + "Type '"
                        + annotationType + "' that does not match the implemented service interface " + direction + " type '"
                        + reactiveType + "'.");
                return null;
            }
            return annotationType;
        }
        return reactiveType;
    }

    private ClassName resolveInternalMapper(
            PipelineCompilationContext ctx,
            String stepName,
            String fieldName,
            ClassName yamlMapper,
            ClassName annotationMapper,
            TypeName expectedDomainType) {
        if (yamlMapper != null && annotationMapper != null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Internal step '" + stepName + "' declares " + fieldName + " '" + yamlMapper.canonicalName()
                    + "' in YAML and deprecated @PipelineStep metadata declares '"
                    + annotationMapper.canonicalName() + "'. YAML is authoritative.");
        }
        ClassName effective = yamlMapper != null ? yamlMapper : annotationMapper;
        if (effective == null) {
            return null;
        }
        boolean yamlOwned = yamlMapper != null;
        if (!validateInternalMapper(ctx, stepName, fieldName, effective, expectedDomainType, yamlOwned)) {
            return INVALID_CLASS_NAME;
        }
        return effective;
    }

    private boolean validateInternalMapper(
            PipelineCompilationContext ctx,
            String stepName,
            String fieldName,
            ClassName mapperClass,
            TypeName expectedDomainType,
            boolean yamlOwned) {
        TypeElement mapperElement = ctx.getProcessingEnv().getElementUtils().getTypeElement(mapperClass.canonicalName());
        if (mapperElement == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal step '" + stepName + "' references " + fieldName + " '" + mapperClass.canonicalName()
                    + "', but the mapper class could not be resolved.");
            return false;
        }

        DeclaredType mapperType = findReactiveSupertype(
            ctx.getProcessingEnv().getTypeUtils(),
            mapperElement.asType(),
            "org.pipelineframework.mapper.Mapper");
        if (mapperType == null || mapperType.getTypeArguments().size() != 2) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal step '" + stepName + "' " + fieldName + " '" + mapperClass.canonicalName()
                    + "' must implement Mapper<Domain, External>."
                    + (yamlOwned ? "" : " Deprecated annotation-sourced mappers are validated the same way."));
            return false;
        }

        TypeName mapperDomainType = TypeName.get(mapperType.getTypeArguments().getFirst());
        if (expectedDomainType != null && !expectedDomainType.equals(mapperDomainType)) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Internal step '" + stepName + "' " + fieldName + " '" + mapperClass.canonicalName()
                    + "' must declare Mapper<" + expectedDomainType + ", External>, but found Mapper<"
                    + mapperDomainType + ", External>."
                    + (yamlOwned ? "" : " Deprecated annotation-sourced mappers must also match the reactive domain type."));
            return false;
        }

        return true;
    }

    private ClassName castToClassName(TypeName typeName) {
        if (typeName instanceof ClassName className) {
            return className;
        }
        return null;
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

    /**
     * Boxes primitive TypeName values to their wrapper types.
     * This ensures model types are always non-primitive, preventing invalid generic signatures.
     */
    private static TypeName boxIfPrimitive(TypeName typeName) {
        if (typeName.isPrimitive()) {
            if (typeName.equals(TypeName.INT)) {
                return TypeName.get(Integer.class);
            } else if (typeName.equals(TypeName.LONG)) {
                return TypeName.get(Long.class);
            } else if (typeName.equals(TypeName.BOOLEAN)) {
                return TypeName.get(Boolean.class);
            } else if (typeName.equals(TypeName.DOUBLE)) {
                return TypeName.get(Double.class);
            } else if (typeName.equals(TypeName.FLOAT)) {
                return TypeName.get(Float.class);
            } else if (typeName.equals(TypeName.SHORT)) {
                return TypeName.get(Short.class);
            } else if (typeName.equals(TypeName.BYTE)) {
                return TypeName.get(Byte.class);
            } else if (typeName.equals(TypeName.CHAR)) {
                return TypeName.get(Character.class);
            }
        } else if (typeName.equals(TypeName.VOID)) {
            return TypeName.get(Void.class);
        }
        return typeName;
    }

    private record ReactiveSignature(StreamingShape shape, TypeName inputType, TypeName outputType) {
    }

    private record SupportedServiceSignature(
        ServiceApiKind apiKind,
        StreamingShape shape,
        TypeName inputType,
        TypeName outputType,
        ReactiveReturnKind reactiveReturnKind,
        String materializingWarning
    ) {
    }

    private List<PipelineStepModel> deduplicateByServiceName(List<PipelineStepModel> stepModels) {
        Map<String, PipelineStepModel> uniqueByServiceName = new LinkedHashMap<>();
        for (PipelineStepModel model : stepModels) {
            String key = model.serviceName() + "::" + String.valueOf(model.deploymentRole());
            // Keep first occurrence so concrete @PipelineStep models take precedence over template fallbacks.
            uniqueByServiceName.putIfAbsent(key, model);
        }
        return new ArrayList<>(uniqueByServiceName.values());
    }
}
