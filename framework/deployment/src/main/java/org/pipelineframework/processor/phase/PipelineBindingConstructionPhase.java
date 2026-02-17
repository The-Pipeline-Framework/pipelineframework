package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.util.DescriptorFileLocator;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;

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
     * Creates a new PipelineBindingConstructionPhase.
     */
    public PipelineBindingConstructionPhase() {
        this(new GrpcRequirementEvaluator());
    }

    /**
     * Creates a new PipelineBindingConstructionPhase with an injected gRPC requirement evaluator.
     *
     * @param grpcRequirementEvaluator evaluator used to determine if descriptor loading is required
     */
    public PipelineBindingConstructionPhase(GrpcRequirementEvaluator grpcRequirementEvaluator) {
        this.grpcRequirementEvaluator = Objects.requireNonNull(grpcRequirementEvaluator, "grpcRequirementEvaluator");
    }

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

        GrpcBindingResolver grpcBindingResolver = new GrpcBindingResolver();
        RestBindingResolver restBindingResolver = new RestBindingResolver();
        
        // Process each step model to construct appropriate bindings
        for (PipelineStepModel model : ctx.getStepModels()) {
            String modelKey = model.serviceName();

            // Delegated steps also need external adapters in addition to regular client bindings.
            if (model.delegateService() != null) {
                ExternalAdapterBinding externalAdapterBinding = new ExternalAdapterBinding(
                    model,
                    model.serviceName(),
                    model.servicePackage(),
                    model.delegateService().toString(),
                    model.externalMapper() != null ? model.externalMapper().toString() : null
                );
                bindingsMap.put(modelKey + "_external_adapter", externalAdapterBinding);
            }

            // Construct gRPC binding if needed
            GrpcBinding grpcBinding = null;
            if (model.delegateService() == null
                && (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP))) {
                grpcBinding = grpcBindingResolver.resolve(model, descriptorSet);
            }

            // Construct REST binding if needed
            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, ctx.getProcessingEnv());
            }

            // Store bindings for this model conditionally
            if (grpcBinding != null) {
                bindingsMap.put(modelKey + "_grpc", grpcBinding);
            }
            if (restBinding != null) {
                bindingsMap.put(modelKey + "_rest", restBinding);
            }
            if (model.enabledTargets().contains(GenerationTarget.LOCAL_CLIENT_STEP)) {
                bindingsMap.put(modelKey + "_local", new LocalBinding(model));
            }
        }
        
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
     * Determine whether gRPC bindings are required for the given compilation context.
     *
     * Evaluates step models and orchestrator template configuration to decide if descriptor
     * loading and gRPC binding resolution are necessary.
     *
     * @param ctx the pipeline compilation context to inspect
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

    private DescriptorProtos.FileDescriptorSet loadDescriptorSet(PipelineCompilationContext ctx) throws IOException {
        DescriptorFileLocator locator = new DescriptorFileLocator();
        Set<String> expectedServices = ctx.getStepModels().stream()
            .filter(model -> model.delegateService() == null)
            .map(PipelineStepModel::serviceName)
            .collect(java.util.stream.Collectors.toSet());
        return locator.locateAndLoadDescriptors(
            ctx.getProcessingEnv().getOptions(),
            expectedServices,
            ctx.getProcessingEnv().getMessager());
    }
}
