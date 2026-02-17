package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.*;
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
    private final StepBindingConstructionService stepBindingConstructionService;

    /**
     * Creates a PipelineBindingConstructionPhase initialized with a default GrpcRequirementEvaluator.
     */
    public PipelineBindingConstructionPhase() {
        this(new GrpcRequirementEvaluator(), new StepBindingConstructionService());
    }

    /**
     * Construct a PipelineBindingConstructionPhase using the given gRPC requirement evaluator.
     *
     * @param grpcRequirementEvaluator evaluator used to decide whether gRPC descriptor loading and binding resolution are required
     * @throws NullPointerException if {@code grpcRequirementEvaluator} is null
     */
    public PipelineBindingConstructionPhase(GrpcRequirementEvaluator grpcRequirementEvaluator) {
        this(grpcRequirementEvaluator, new StepBindingConstructionService());
    }

    PipelineBindingConstructionPhase(
            GrpcRequirementEvaluator grpcRequirementEvaluator,
            StepBindingConstructionService stepBindingConstructionService) {
        this.grpcRequirementEvaluator = Objects.requireNonNull(grpcRequirementEvaluator, "grpcRequirementEvaluator");
        this.stepBindingConstructionService = Objects.requireNonNull(
            stepBindingConstructionService,
            "stepBindingConstructionService");
    }

    /**
     * Identifier for the pipeline binding construction phase.
     *
     * @return the phase name "Pipeline Binding Construction Phase"
     */
    @Override
    public String name() {
        return "Pipeline Binding Construction Phase";
    }

    /**
     * Constructs renderer-specific bindings for each pipeline step and stores them in the compilation context.
     *
     * <p>This builds gRPC, REST and local bindings as appropriate for each PipelineStepModel, optionally
     * loads a protocol descriptor set when gRPC bindings are required, and adds an orchestrator binding
     * if orchestrator models are present. Bindings are stored in the context's renderer bindings map
     * using keys: {@code <modelName>_grpc}, {@code <modelName>_rest},
     * {@code <modelName>_local} and {@code orchestrator}.
     *
     * @param ctx the pipeline compilation context to read models from and write constructed bindings into
     * @throws Exception if descriptor set loading or binding construction fails
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Create a map to store bindings for each model
        Map<String, Object> bindingsMap = new HashMap<>();

        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();

        if (descriptorSet == null && needsGrpcBindings(ctx)) {
            descriptorSet = loadDescriptorSet(ctx);
            ctx.setDescriptorSet(descriptorSet);
        }

        bindingsMap.putAll(stepBindingConstructionService.buildBindings(ctx, descriptorSet));
        
        // Process orchestrator models if present
        if (!ctx.getOrchestratorModels().isEmpty()) {
            OrchestratorBinding orchestratorBinding = OrchestratorBindingBuilder.buildOrchestratorBinding(
                (PipelineTemplateConfig) ctx.getPipelineTemplateConfig(),
                ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class) : Set.of()
            );
            if (orchestratorBinding != null) {
                bindingsMap.put("orchestrator", orchestratorBinding);
            }
        }
        
        // Store the bindings map in the context
        ctx.setRendererBindings(bindingsMap);
    }

    /**
     * Determine whether the compilation context requires gRPC bindings.
     *
     * Considers step models, orchestrator models, and the pipeline template configuration to decide
     * if descriptor loading and gRPC binding resolution are needed.
     *
     * @param ctx the pipeline compilation context to inspect for models and configuration
     * @return `true` if gRPC bindings are required, `false` otherwise
     */
    private boolean needsGrpcBindings(PipelineCompilationContext ctx) {
        PipelineTemplateConfig config = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig cfg ? cfg : null;
        return grpcRequirementEvaluator.needsGrpcBindings(
            ctx.getStepModels(),
            ctx.getOrchestratorModels(),
            config,
            ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getMessager() : null
        );
    }

    /**
     * Load the protobuf DescriptorProtos.FileDescriptorSet for pipeline step models that require gRPC descriptors.
     *
     * @param ctx the compilation context providing step models and the processing environment
     * @return the descriptor set containing descriptors for non-delegated step services
     * @throws IOException if descriptor files cannot be located or read
     */
    private DescriptorProtos.FileDescriptorSet loadDescriptorSet(PipelineCompilationContext ctx) throws IOException {
        DescriptorFileLocator locator = new DescriptorFileLocator();
        Set<String> expectedServices = ctx.getStepModels().stream()
            .filter(model -> model.delegateService() == null)
            .map(PipelineStepModel::serviceName)
            .collect(Collectors.toSet());
        return locator.locateAndLoadDescriptors(
            ctx.getProcessingEnv().getOptions(),
            expectedServices,
            ctx.getProcessingEnv().getMessager());
    }

}
