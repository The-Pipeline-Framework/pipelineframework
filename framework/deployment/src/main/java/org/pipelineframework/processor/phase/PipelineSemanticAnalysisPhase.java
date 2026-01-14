package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Set;
import javax.lang.model.element.Element;

import org.pipelineframework.annotation.PipelineOrchestrator;
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

        // Analyze streaming shapes and other semantic properties
        // This phase focuses on semantic analysis without building bindings or calling renderers
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
