package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.DescriptorFileLocator;

/**
 * Constructs renderer-specific bindings from semantic models and resolved targets.
 * This phase reads semantic models and resolved targets, then constructs the appropriate
 * renderer-specific bindings and stores them in the compilation context.
 * <p>
 * Bindings are immutable and scoped to one renderer, never modifying semantic models.
 */
public class PipelineBindingConstructionPhase implements PipelineCompilationPhase {

    private final GrpcRequirementEvaluator grpcRequirementEvaluator;

    /**
     * Creates a new PipelineBindingConstructionPhase with default collaborators.
     */
    public PipelineBindingConstructionPhase() {
        this(new GrpcRequirementEvaluator());
    }

    /**
     * Creates a new PipelineBindingConstructionPhase with explicit collaborators.
     */
    PipelineBindingConstructionPhase(GrpcRequirementEvaluator grpcRequirementEvaluator) {
        this.grpcRequirementEvaluator = Objects.requireNonNull(grpcRequirementEvaluator, "grpcRequirementEvaluator must not be null");
    }

    @Override
    public String name() {
        return "Pipeline Binding Construction Phase";
    }

    /**
     * Constructs renderer-specific bindings for each pipeline step and stores them in the compilation context.
     *
     * @param ctx the pipeline compilation context to read models from and write constructed bindings into
     * @throws Exception if descriptor set loading or binding construction fails
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();

        // Evaluate gRPC requirement and load descriptors if needed
        PipelineTemplateConfig templateConfig = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig cfg ? cfg : null;
        if (descriptorSet == null && ctx.getProcessingEnv() != null && grpcRequirementEvaluator.needsGrpcBindings(
                ctx.getStepModels(), ctx.getOrchestratorModels(),
                templateConfig,
                ctx.getProcessingEnv().getMessager())) {
            descriptorSet = loadDescriptorSet(ctx);
            ctx.setDescriptorSet(descriptorSet);
        }

        // Construct step bindings
        Map<String, Object> bindingsMap = StepBindingBuilder.constructBindings(
            ctx.getStepModels(), descriptorSet, ctx.getProcessingEnv());

        // Add orchestrator binding if present
        if (!ctx.getOrchestratorModels().isEmpty()) {
            OrchestratorBinding orchestratorBinding = OrchestratorBindingBuilder.buildOrchestratorBinding(
                templateConfig,
                ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class) : Set.of()
            );
            if (orchestratorBinding != null) {
                bindingsMap.put(StepBindingBuilder.ORCHESTRATOR_KEY, orchestratorBinding);
            }
        }

        ctx.setRendererBindings(bindingsMap);
    }

    private DescriptorProtos.FileDescriptorSet loadDescriptorSet(PipelineCompilationContext ctx) throws IOException {
        DescriptorFileLocator locator = new DescriptorFileLocator();
        Set<String> expectedServices = ctx.getStepModels().stream()
            .map(PipelineStepModel::serviceName)
            .collect(java.util.stream.Collectors.toSet());
        return locator.locateAndLoadDescriptors(
            ctx.getProcessingEnv().getOptions(),
            expectedServices,
            ctx.getProcessingEnv().getMessager());
    }
}
