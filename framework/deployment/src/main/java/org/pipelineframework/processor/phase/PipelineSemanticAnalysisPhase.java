package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Set;
import javax.lang.model.element.Element;
import javax.tools.Diagnostic;

import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Performs semantic analysis and policy decisions on discovered models.
 * This phase analyzes semantic models, sets flags and derived values in the context,
 * and emits errors or warnings via Messager if needed.
 */
public class PipelineSemanticAnalysisPhase implements PipelineCompilationPhase {

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

        // Analyze streaming shapes and other semantic properties
        // This phase focuses on semantic analysis without building bindings or calling renderers
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

    private void validateProviderHints(PipelineCompilationContext ctx) {
        if (ctx == null || ctx.getProcessingEnv() == null) {
            return;
        }
        String policy = ctx.getProcessingEnv().getOptions().get("pipeline.parallelism");
        String normalizedPolicy = policy == null ? null : policy.trim().toUpperCase();
        var options = ctx.getProcessingEnv().getOptions();
        for (var entry : options.entrySet()) {
            String key = entry.getKey();
            if (key == null || !key.startsWith("pipeline.provider.class.")) {
                continue;
            }
            String providerClass = entry.getValue();
            if (providerClass == null || providerClass.isBlank()) {
                continue;
            }
            String label = "Provider '" + key.substring("pipeline.provider.class.".length()) + "'";
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
        if ("EXPANSION".equalsIgnoreCase(cardinality)) {
            return StreamingShape.UNARY_STREAMING;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return StreamingShape.STREAMING_UNARY;
        }
        if ("MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return StreamingShape.STREAMING_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }

    /**
     * Checks if the input cardinality is streaming.
     *
     * @param cardinality the cardinality string
     * @return true if the input is streaming, false otherwise
     */
    protected boolean isStreamingInputCardinality(String cardinality) {
        return "REDUCTION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality);
    }

    /**
     * Applies cardinality to determine if streaming should continue.
     *
     * @param cardinality the cardinality string
     * @param currentStreaming the current streaming state
     * @return the updated streaming state
     */
    protected boolean applyCardinalityToStreaming(String cardinality, boolean currentStreaming) {
        if ("EXPANSION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return true;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return false;
        }
        return currentStreaming;
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
