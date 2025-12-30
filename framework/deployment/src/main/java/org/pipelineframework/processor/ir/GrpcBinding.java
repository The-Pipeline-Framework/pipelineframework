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

        public Builder model(PipelineStepModel model) {
            this.model = model;
            return this;
        }

        public Builder serviceDescriptor(Object serviceDescriptor) {
            this.serviceDescriptor = serviceDescriptor;
            return this;
        }

        public Builder methodDescriptor(Object methodDescriptor) {
            this.methodDescriptor = methodDescriptor;
            return this;
        }

        public GrpcBinding build() {
            if (model == null) {
                throw new IllegalStateException("model is required");
            }
            return new GrpcBinding(model, serviceDescriptor, methodDescriptor);
        }
    }
}
