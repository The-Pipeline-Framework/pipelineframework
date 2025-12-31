package org.pipelineframework.processor.ir;

/**
 * Represents resolved gRPC bindings for a pipeline step.
 *
 * @param model Reference to the semantic model this binding is based on
 * @param serviceDescriptor The gRPC service descriptor from the protobuf descriptor set
 * @param methodDescriptor The gRPC method descriptor for the remoteProcess method
 */
public record GrpcBinding(
        PipelineStepModel model,
        Object serviceDescriptor,  // Using Object to avoid direct dependency on protobuf types in the record
        Object methodDescriptor    // Using Object to avoid direct dependency on protobuf types in the record
) implements PipelineBinding {
    /**
     * Creates a new GrpcBinding instance.
     *
     * @param model the semantic model this binding is based on
     * @param serviceDescriptor the gRPC service descriptor
     * @param methodDescriptor the gRPC method descriptor for remoteProcess
     */
    public GrpcBinding {
        if (model == null) {
            throw new IllegalArgumentException("model cannot be null");
        }
    }

    /**
     * Builder class for creating GrpcBinding instances.
     */
    public static class Builder {
        private PipelineStepModel model;
        private Object serviceDescriptor;
        private Object methodDescriptor;

        /**
         * Set the PipelineStepModel to be used when building a GrpcBinding.
         *
         * @param model the semantic PipelineStepModel for the binding
         * @return      this Builder instance for fluent chaining
         */
        public Builder model(PipelineStepModel model) {
            this.model = model;
            return this;
        }

        /**
         * Assigns the gRPC service descriptor to include in the resulting GrpcBinding.
         *
         * @param serviceDescriptor the gRPC service descriptor (kept as `Object` to avoid a direct protobuf dependency)
         * @return                  this Builder for fluent chaining
         */
        public Builder serviceDescriptor(Object serviceDescriptor) {
            this.serviceDescriptor = serviceDescriptor;
            return this;
        }

        /**
         * Set the gRPC method descriptor used by the binding.
         *
         * @param methodDescriptor the gRPC method descriptor for the remote method (kept as `Object` to avoid a direct protobuf dependency)
         * @return the builder instance
         */
        public Builder methodDescriptor(Object methodDescriptor) {
            this.methodDescriptor = methodDescriptor;
            return this;
        }

        /**
         * Create a GrpcBinding populated with the builder's values.
         *
         * @return a GrpcBinding instance constructed from the builder's state
         * @throws IllegalStateException if the required `model` has not been set
         */
        public GrpcBinding build() {
            if (model == null) {
                throw new IllegalStateException("model is required");
            }
            return new GrpcBinding(model, serviceDescriptor, methodDescriptor);
        }
    }
}