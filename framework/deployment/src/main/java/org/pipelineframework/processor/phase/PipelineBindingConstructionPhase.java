package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

    /**
     * Creates a new PipelineBindingConstructionPhase.
     */
    public PipelineBindingConstructionPhase() {
    }

    @Override
    public String name() {
        return "Pipeline Binding Construction Phase";
    }

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
            // Construct gRPC binding if needed
            GrpcBinding grpcBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP)) {
                grpcBinding = grpcBindingResolver.resolve(model, descriptorSet);
            }
            
            // Construct REST binding if needed
            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, ctx.getProcessingEnv());
            }
            
            // Store bindings for this model
            String modelKey = model.serviceName();
            bindingsMap.put(modelKey + "_grpc", grpcBinding);
            bindingsMap.put(modelKey + "_rest", restBinding);
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

    private boolean needsGrpcBindings(PipelineCompilationContext ctx) {
        if (ctx.getStepModels().stream().anyMatch(model ->
            model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP))) {
            return true;
        }
        if (!ctx.getOrchestratorModels().isEmpty()) {
            PipelineTemplateConfig config = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig cfg ? cfg : null;
            if (config == null) {
                return false;
            }
            String transport = config.transport();
            if (transport == null || transport.isBlank()) {
                return true;
            }
            return TransportMode.fromStringOptional(transport)
                .map(mode -> mode == TransportMode.GRPC)
                .orElse(false);
        }
        return false;
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
