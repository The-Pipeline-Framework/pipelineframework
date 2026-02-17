package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Performs semantic analysis and policy decisions on discovered models.
 * This phase analyzes semantic models, sets flags and derived values in the context,
 * and emits errors or warnings via Messager if needed.
 */
public class PipelineSemanticAnalysisPhase implements PipelineCompilationPhase {
    private static final List<String> REACTIVE_SERVICE_INTERFACE_NAMES = List.of(
        "org.pipelineframework.service.ReactiveService",
        "org.pipelineframework.service.ReactiveStreamingService",
        "org.pipelineframework.service.ReactiveStreamingClientService",
        "org.pipelineframework.service.ReactiveClientStreamingService",
        "org.pipelineframework.service.ReactiveBidirectionalStreamingService");

    /**
     * Creates a new PipelineSemanticAnalysisPhase.
     */
    public PipelineSemanticAnalysisPhase() {
    }

    @Override
    public String name() {
        return "Pipeline Semantic Analysis Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Analyze aspects to identify those that should be expanded
        List<PipelineAspectModel> aspectsForExpansion = List.copyOf(ctx.getAspectModels());
        ctx.setAspectsForExpansion(aspectsForExpansion);

        // Determine if orchestrator should be generated
        boolean shouldGenerateOrchestrator = shouldGenerateOrchestrator(ctx);
        ctx.setOrchestratorGenerated(shouldGenerateOrchestrator);

        validateParallelismHints(ctx);
        validateProviderHints(ctx);
        validateFunctionPlatformConstraints(ctx);
        validateYamlDrivenSteps(ctx);

        // Analyze streaming shapes and other semantic properties
        // This phase focuses on semantic analysis without building bindings or calling renderers
    }

    private void validateFunctionPlatformConstraints(PipelineCompilationContext ctx) {
        if (ctx == null || ctx.getProcessingEnv() == null || !ctx.isPlatformModeFunction()) {
            return;
        }
        if (!ctx.isTransportModeRest()) {
            ctx.getProcessingEnv().getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "pipeline.platform=FUNCTION currently requires pipeline.transport=REST.");
            return;
        }
        List<PipelineStepModel> steps = ctx.getStepModels();
        if (steps == null || steps.isEmpty()) {
            return;
        }
    }

    private void validateParallelismHints(PipelineCompilationContext ctx) {
        if (ctx == null || ctx.getProcessingEnv() == null) {
            return;
        }
        List<PipelineAspectModel> aspects = ctx.getAspectModels();
        if (aspects == null || aspects.isEmpty()) {
            return;
        }

        String policy = ctx.getProcessingEnv().getOptions().get("pipeline.parallelism");
        String normalizedPolicy = policy == null ? null : policy.trim().toUpperCase();

        for (PipelineAspectModel aspect : aspects) {
            if (aspect == null || aspect.config() == null) {
                continue;
            }
            Object implValue = aspect.config().get("pluginImplementationClass");
            if (implValue == null) {
                continue;
            }
            String implClass = String.valueOf(implValue).trim();
            if (implClass.isEmpty()) {
                continue;
            }
            var elementUtils = ctx.getProcessingEnv().getElementUtils();
            var typeElement = elementUtils.getTypeElement(implClass);
            if (typeElement == null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "Plugin implementation class '" + implClass + "' not found for aspect '" + aspect.name() + "'");
                continue;
            }

            Object providerClass = aspect.config().get("providerClass");
            if (providerClass != null && !String.valueOf(providerClass).isBlank()) {
                validateProviderHint(ctx, String.valueOf(providerClass).trim(),
                    "Aspect '" + aspect.name() + "' provider", normalizedPolicy);
                continue;
            }

            ParallelismHint hint = typeElement.getAnnotation(ParallelismHint.class);
            if (hint == null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "Plugin implementation class '" + implClass + "' does not declare @ParallelismHint " +
                        "and no providerClass is configured for aspect '" + aspect.name() + "'.");
                continue;
            }

            OrderingRequirement ordering = hint.ordering();
            ThreadSafety threadSafety = hint.threadSafety();

            boolean policyKnown = normalizedPolicy != null && !normalizedPolicy.isBlank();
            boolean sequentialPolicy = "SEQUENTIAL".equals(normalizedPolicy);
            boolean autoPolicy = "AUTO".equals(normalizedPolicy);
            boolean parallelPolicy = "PARALLEL".equals(normalizedPolicy);

            if (threadSafety == ThreadSafety.UNSAFE) {
                if (policyKnown && !sequentialPolicy) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Plugin '" + implClass + "' is not thread-safe. " +
                            "Set pipeline.parallelism=SEQUENTIAL to use aspect '" + aspect.name() + "'.");
                } else if (!policyKnown) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.WARNING,
                        "Plugin '" + implClass + "' is not thread-safe. " +
                            "Set pipeline.parallelism=SEQUENTIAL to use aspect '" + aspect.name() + "'.");
                }
            }

            if (ordering == OrderingRequirement.STRICT_REQUIRED) {
                if (policyKnown && !sequentialPolicy) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Plugin '" + implClass + "' requires strict ordering. " +
                            "Set pipeline.parallelism=SEQUENTIAL to use aspect '" + aspect.name() + "'.");
                } else if (!policyKnown) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.WARNING,
                        "Plugin '" + implClass + "' requires strict ordering. " +
                            "Set pipeline.parallelism=SEQUENTIAL to use aspect '" + aspect.name() + "'.");
                }
            }

            if (ordering == OrderingRequirement.STRICT_ADVISED) {
                if (!policyKnown) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.WARNING,
                        "Plugin '" + implClass + "' advises strict ordering for aspect '" + aspect.name() + "'. " +
                            "AUTO will run sequentially; PARALLEL will override the advice.");
                } else if (autoPolicy) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.WARNING,
                        "Plugin '" + implClass + "' advises strict ordering; AUTO will run sequentially " +
                            "for aspect '" + aspect.name() + "'.");
                } else if (parallelPolicy) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        Diagnostic.Kind.WARNING,
                        "Plugin '" + implClass + "' advises strict ordering; PARALLEL overrides the advice " +
                            "for aspect '" + aspect.name() + "'.");
                }
            }
        }
    }

    /**
     * Validates provider-related processing-option hints and invokes provider validation for each recognized provider entry.
     *
     * Reads the global parallelism option and, for each processing option that names a provider, maps the option key to a provider label
     * ("persistence.provider.class" -> "Provider 'persistence'", keys starting with "pipeline.provider.class." -> "Provider '<name>'")
     * and calls validateProviderHint with the provider class, label, and the normalized policy.
     *
     * @param ctx the pipeline compilation context containing the processing environment and options; if null or its processing environment
     *            is null, no validation is performed
     */
    private void validateProviderHints(PipelineCompilationContext ctx) {
        if (ctx == null || ctx.getProcessingEnv() == null) {
            return;
        }
        String policy = ctx.getProcessingEnv().getOptions().get("pipeline.parallelism");
        String normalizedPolicy = policy == null ? null : policy.trim().toUpperCase();
        var options = ctx.getProcessingEnv().getOptions();
        for (var entry : options.entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            String providerClass = entry.getValue();
            if (providerClass == null || providerClass.isBlank()) {
                continue;
            }
            String label;
            if ("persistence.provider.class".equals(key)) {
                label = "Provider 'persistence'";
            } else if (key.startsWith("pipeline.provider.class.")) {
                label = "Provider '" + key.substring("pipeline.provider.class.".length()) + "'";
            } else {
                continue;
            }
            validateProviderHint(ctx, providerClass.trim(), label, normalizedPolicy);
        }
    }

    private void validateProviderHint(PipelineCompilationContext ctx,
                                      String trimmedClass,
                                      String label,
                                      String normalizedPolicy) {
        var elementUtils = ctx.getProcessingEnv().getElementUtils();
        var typeElement = elementUtils.getTypeElement(trimmedClass);
        if (typeElement == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                label + " class '" + trimmedClass + "' not found for processing option");
            return;
        }

        ParallelismHint hint = typeElement.getAnnotation(ParallelismHint.class);
        if (hint == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                Diagnostic.Kind.WARNING,
                label + " class '" + trimmedClass + "' does not declare @ParallelismHint; " +
                    "parallelism ordering/thread-safety cannot be validated at build time.");
            return;
        }

        OrderingRequirement ordering = hint.ordering();
        ThreadSafety threadSafety = hint.threadSafety();

        boolean policyKnown = normalizedPolicy != null && !normalizedPolicy.isBlank();
        boolean sequentialPolicy = "SEQUENTIAL".equals(normalizedPolicy);
        boolean autoPolicy = "AUTO".equals(normalizedPolicy);
        boolean parallelPolicy = "PARALLEL".equals(normalizedPolicy);

        if (threadSafety == ThreadSafety.UNSAFE) {
            if (policyKnown && !sequentialPolicy) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    label + " '" + trimmedClass + "' is not thread-safe. " +
                        "Set pipeline.parallelism=SEQUENTIAL.");
            } else if (!policyKnown) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    label + " '" + trimmedClass + "' is not thread-safe. " +
                        "Set pipeline.parallelism=SEQUENTIAL.");
            }
        }

        if (ordering == OrderingRequirement.STRICT_REQUIRED) {
            if (policyKnown && !sequentialPolicy) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    label + " '" + trimmedClass + "' requires strict ordering. " +
                        "Set pipeline.parallelism=SEQUENTIAL.");
            } else if (!policyKnown) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    label + " '" + trimmedClass + "' requires strict ordering. " +
                        "Set pipeline.parallelism=SEQUENTIAL.");
            }
        }

        if (ordering == OrderingRequirement.STRICT_ADVISED) {
            if (!policyKnown) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    label + " '" + trimmedClass + "' advises strict ordering. " +
                        "AUTO will run sequentially; PARALLEL will override the advice.");
            } else if (autoPolicy) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    label + " '" + trimmedClass + "' advises strict ordering; AUTO will run sequentially.");
            } else if (parallelPolicy) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    label + " '" + trimmedClass + "' advises strict ordering; PARALLEL overrides the advice.");
            }
        }
    }

    /**
     * Determines the streaming shape based on cardinality.
     *
     * @param cardinality the cardinality string
     * @return the corresponding streaming shape
     */
    protected StreamingShape streamingShape(String cardinality) {
        return StreamingShapeResolver.streamingShape(cardinality);
    }

    /**
     * Checks if the input cardinality is streaming.
     *
     * @param cardinality the cardinality string
     * @return true if the input is streaming, false otherwise
     */
    protected boolean isStreamingInputCardinality(String cardinality) {
        return StreamingShapeResolver.isStreamingInputCardinality(cardinality);
    }

    /**
     * Applies cardinality to determine if streaming should continue.
     *
     * @param cardinality the cardinality string
     * @param currentStreaming the current streaming state
     * @return the updated streaming state
     */
    protected boolean applyCardinalityToStreaming(String cardinality, boolean currentStreaming) {
        return StreamingShapeResolver.applyCardinalityToStreaming(cardinality, currentStreaming);
    }

    /**
     * Checks if the aspect is a cache aspect.
     *
     * @param aspect the aspect model to check
     * @return true if it's a cache aspect, false otherwise
     */
    protected boolean isCacheAspect(PipelineAspectModel aspect) {
        return "cache".equalsIgnoreCase(aspect.name());
    }

    /**
     * Determines if orchestrator should be generated based on annotations and options.
     *
     * @param ctx the compilation context
     * @return true if orchestrator should be generated, false otherwise
     */
    protected boolean shouldGenerateOrchestrator(PipelineCompilationContext ctx) {
        // Check if there are orchestrator elements annotated
        Set<? extends Element> orchestratorElements = 
            ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class) : Set.of();
        
        if (orchestratorElements != null && !orchestratorElements.isEmpty()) {
            return true;
        }
        
        // Check processing option
        String option = ctx.getProcessingEnv() != null ? 
            ctx.getProcessingEnv().getOptions().get("pipeline.orchestrator.generate") : null;
        if (option == null || option.isBlank()) {
            return false;
        }
        String normalized = option.trim();
        return "true".equalsIgnoreCase(normalized) || "1".equals(normalized);
    }

    /**
     * Validates YAML-driven steps to ensure they meet the requirements.
     *
     * @param ctx the compilation context
     */
    private void validateYamlDrivenSteps(PipelineCompilationContext ctx) {
        if (ctx == null || ctx.getProcessingEnv() == null || ctx.getStepModels() == null) {
            return;
        }

        var elementUtils = ctx.getProcessingEnv().getElementUtils();
        var messager = ctx.getProcessingEnv().getMessager();

        // Validate that annotated services not referenced in YAML don't generate steps
        // This is done by checking if each annotated service is in the YAML step definitions
        List<String> yamlReferencedServices = new ArrayList<>();
        if (ctx.getStepDefinitions() != null) {
            for (var stepDef : ctx.getStepDefinitions()) {
                if (stepDef.kind() == org.pipelineframework.processor.ir.StepKind.INTERNAL
                        && stepDef.executionClass() != null) {
                    yamlReferencedServices.add(stepDef.executionClass().canonicalName());
                }
            }
        }

        // Get all @PipelineStep annotated classes
        Set<? extends Element> pipelineStepElements =
            ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(org.pipelineframework.annotation.PipelineStep.class) : Set.of();

        // Check if we should warn about unreferenced steps (default: true)
        boolean warnUnreferenced = true;
        String warnOption = ctx.getProcessingEnv().getOptions().get("pipeline.warnUnreferencedSteps");
        if (warnOption != null) {
            warnUnreferenced = Boolean.parseBoolean(warnOption);
        }

        for (Element annotatedElement : pipelineStepElements) {
            // Validate that @PipelineStep is only applied to classes
            if (annotatedElement.getKind() != javax.lang.model.element.ElementKind.CLASS) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "@PipelineStep can only be applied to classes",
                    annotatedElement);
                continue;
            }

            TypeElement serviceClass = (TypeElement) annotatedElement;
            String serviceClassName = serviceClass.getQualifiedName().toString();

            // If this annotated service is not referenced in YAML, emit a note (not a warning)
            if (!yamlReferencedServices.contains(serviceClassName)) {
                if (warnUnreferenced) {
                    messager.printMessage(
                        Diagnostic.Kind.NOTE,
                        "Service '" + serviceClassName + "' is annotated with @PipelineStep but not referenced in pipeline YAML. No step will be generated for it.");
                }
            }
        }

        // Validate the step models created from YAML
        for (PipelineStepModel model : ctx.getStepModels()) {
            if (model.delegateService() != null) {
                // Validate that the delegate service exists
                var delegateElement = elementUtils.getTypeElement(model.delegateService().canonicalName());
                if (delegateElement == null) {
                    messager.printMessage(
                        Diagnostic.Kind.ERROR,
                        "Delegate service '" + model.delegateService().canonicalName() + "' not found for step '" + model.serviceName() + "'");
                    continue;
                }

                var typeUtils = ctx.getProcessingEnv().getTypeUtils();
                boolean isValidReactiveService = implementsAnyReactiveService(
                    delegateElement,
                    elementUtils,
                    typeUtils);

                if (!isValidReactiveService) {
                    messager.printMessage(
                        Diagnostic.Kind.ERROR,
                        "Delegate service '" + model.delegateService().canonicalName() +
                        "' must implement one of the reactive service interfaces (ReactiveService, ReactiveStreamingService, etc.) for step '" +
                        model.serviceName() + "'");
                }

                // If external mapper is specified, validate it implements ExternalMapper
                if (model.externalMapper() != null) {
                    var externalMapperElement = elementUtils.getTypeElement(model.externalMapper().canonicalName());
                    if (externalMapperElement == null) {
                        messager.printMessage(
                            Diagnostic.Kind.ERROR,
                            "External mapper '" + model.externalMapper().canonicalName() + "' not found for step '" + model.serviceName() + "'");
                        continue;
                    }

                    // Check if the external mapper implements ExternalMapper interface
                    boolean implementsExternalMapper = false;
                    var externalMapperInterfaceElement = elementUtils.getTypeElement("org.pipelineframework.mapper.ExternalMapper");
                    if (externalMapperInterfaceElement != null && typeUtils.isAssignable(externalMapperElement.asType(), externalMapperInterfaceElement.asType())) {
                        implementsExternalMapper = true;
                    }

                    if (!implementsExternalMapper) {
                        messager.printMessage(
                            Diagnostic.Kind.ERROR,
                            "External mapper '" + model.externalMapper().canonicalName() + 
                            "' must implement org.pipelineframework.mapper.ExternalMapper for step '" + 
                            model.serviceName() + "'");
                    }
                } else {
                    DelegateTypeSignature delegateSignature = resolveDelegateTypeSignature(
                        delegateElement,
                        typeUtils,
                        messager,
                        model.serviceName());
                    if (delegateSignature == null || model.inputMapping() == null || model.outputMapping() == null) {
                        continue;
                    }

                    String stepInputType = String.valueOf(model.inputMapping().domainType());
                    String stepOutputType = String.valueOf(model.outputMapping().domainType());
                    String delegateInputType = delegateSignature.inputType().toString();
                    String delegateOutputType = delegateSignature.outputType().toString();

                    boolean inputDiffers = !stepInputType.equals(delegateInputType);
                    boolean outputDiffers = !stepOutputType.equals(delegateOutputType);
                    if (inputDiffers || outputDiffers) {
                        messager.printMessage(
                            Diagnostic.Kind.ERROR,
                            "Delegated step '" + model.serviceName()
                                + "' requires an external mapper because YAML types ["
                                + stepInputType + " -> " + stepOutputType
                                + "] differ from delegate service types ["
                                + delegateInputType + " -> " + delegateOutputType + "].");
                    }
                }
            }
        }
    }

    private DelegateTypeSignature resolveDelegateTypeSignature(
            TypeElement delegateElement,
            Types typeUtils,
            javax.annotation.processing.Messager messager,
            String stepName) {
        List<DeclaredType> matches = new ArrayList<>();
        List<String> matchedInterfaceNames = new ArrayList<>();
        for (String reactiveInterface : REACTIVE_SERVICE_INTERFACE_NAMES) {
            DeclaredType declared = findReactiveSupertype(typeUtils, delegateElement.asType(), reactiveInterface);
            if (declared == null || declared.getTypeArguments().size() < 2) {
                continue;
            }
            matches.add(declared);
            matchedInterfaceNames.add(reactiveInterface);
        }

        if (matches.size() > 1) {
            if (messager != null) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Delegated step '" + stepName + "' uses delegate service '"
                        + delegateElement.getQualifiedName()
                        + "' that implements multiple reactive service interfaces: "
                        + String.join(", ", matchedInterfaceNames)
                        + ". Use exactly one reactive service interface.");
            }
            return null;
        }

        if (matches.isEmpty()) {
            return null;
        }

        DeclaredType match = matches.getFirst();
        return new DelegateTypeSignature(
            match.getTypeArguments().get(0),
            match.getTypeArguments().get(1));
    }

    private boolean implementsAnyReactiveService(
            TypeElement delegateElement,
            javax.lang.model.util.Elements elementUtils,
            Types typeUtils) {
        for (String interfaceName : REACTIVE_SERVICE_INTERFACE_NAMES) {
            TypeElement interfaceElement = elementUtils.getTypeElement(interfaceName);
            if (interfaceElement != null && typeUtils.isAssignable(delegateElement.asType(), interfaceElement.asType())) {
                return true;
            }
        }
        return false;
    }

    private DeclaredType findReactiveSupertype(Types types, TypeMirror type, String targetQualifiedName) {
        if (type == null) {
            return null;
        }
        if (type instanceof DeclaredType declaredType
            && declaredType.asElement() instanceof TypeElement typeElement
            && targetQualifiedName.contentEquals(typeElement.getQualifiedName())) {
            return declaredType;
        }

        for (TypeMirror supertype : types.directSupertypes(type)) {
            DeclaredType match = findReactiveSupertype(types, supertype, targetQualifiedName);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private record DelegateTypeSignature(TypeMirror inputType, TypeMirror outputType) {
    }

    /**
     * Determines if orchestrator CLI should be generated.
     *
     * @param orchestratorElements the set of orchestrator elements
     * @return true if CLI should be generated, false otherwise
     */
    protected boolean shouldGenerateOrchestratorCli(Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return false;
        }
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = element.getAnnotation(PipelineOrchestrator.class);
            if (annotation == null) {
                continue;
            }
            if (annotation.generateCli()) {
                return true;
            }
        }
        return false;
    }
}
