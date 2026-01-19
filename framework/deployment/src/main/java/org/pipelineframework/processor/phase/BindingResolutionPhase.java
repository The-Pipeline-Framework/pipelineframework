package org.pipelineframework.processor.phase;

import java.util.Map;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;

/**
 * Resolves renderer-specific bindings for each model.
 * This phase creates transport-specific bindings that will be used by renderers.
 */
public class BindingResolutionPhase implements PipelineCompilationPhase {

    /**
     * Creates a new BindingResolutionPhase.
     */
    public BindingResolutionPhase() {
    }

    @Override
    public String name() {
        return "Binding Resolution Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Initialize the binding resolvers
        GrpcBindingResolver grpcBindingResolver = new GrpcBindingResolver();
        RestBindingResolver restBindingResolver = new RestBindingResolver();
        
        // Store the resolvers in the context for later use by other phases
        // In a real implementation, this phase would resolve bindings for each model
        // and store them in the context
        
        Map<String, Object> bindings = Map.of(
            "grpcResolver", grpcBindingResolver,
            "restResolver", restBindingResolver
        );
        
        ctx.setRendererBindings(bindings);

        // Actually resolve bindings for each model based on their requirements
        // This would typically involve iterating through the step models and creating appropriate bindings
        // For now, we'll leave this as a placeholder since the actual binding resolution
        // is handled in the PipelineBindingConstructionPhase
    }
}
