package org.pipelineframework.processor.phase;

import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;

/**
 * Builds step-specific bindings (gRPC and REST).
 */
class StepBindingBuilder {

    /**
     * Rebuilds a gRPC binding with the specified model.
     *
     * @param binding the original binding (may be null)
     * @param model the pipeline step model
     * @return a new gRPC binding with the model applied
     */
    static GrpcBinding rebuildGrpcBinding(GrpcBinding binding, PipelineStepModel model) {
        if (binding == null) {
            // Create a basic binding based on the model
            return new GrpcBinding(
                model,
                null, // service descriptor would be resolved in actual implementation
                null  // method descriptor would be resolved in actual implementation
            );
        }
        return new GrpcBinding(model, binding.serviceDescriptor(), binding.methodDescriptor());
    }

    /**
     * Rebuilds a REST binding with the specified model.
     *
     * @param binding the original binding (may be null)
     * @param model the pipeline step model
     * @return a new REST binding with the model applied
     */
    static RestBinding rebuildRestBinding(RestBinding binding, PipelineStepModel model) {
        if (binding == null) {
            // Create a basic binding based on the model
            return new RestBinding(
                model,
                null // rest path override would be resolved in actual implementation
            );
        }
        return new RestBinding(model, binding.restPathOverride());
    }
}