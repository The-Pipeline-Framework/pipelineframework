package org.pipelineframework.processor.ir;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

/**
 * Intermediate representation containing only semantic information derived from @PipelineStep annotations.
 * This class captures all the essential information needed to generate pipeline artifacts.
 */
public class PipelineStepIR {
    private final String serviceName;
    private final String servicePackage;
    private final ClassName serviceClassName;
    
    // Directional type mappings
    private final TypeMapping inputMapping;    // Domain -> gRPC
    private final TypeMapping outputMapping;   // Domain -> gRPC
    
    // Semantic configuration
    private final StreamingShape streamingShape;
    private final StepKind stepKind;
    private final Set<GenerationTarget> enabledTargets;
    private final ExecutionMode executionMode;
    
    // Configuration for REST generation
    private final String restPath;  // Only if REST enabled, can be null

    // Configuration for gRPC implementation
    private final TypeName grpcImplType;  // The gRPC implementation class
    private final TypeName grpcStubType;  // The gRPC stub class for client

    // Configuration for gRPC client
    private final String grpcClientName;  // The gRPC client name for @GrpcClient annotation
    
    /**
     * Creates a new PipelineStepIR instance.
     *
     * @param serviceName the name of the service
     * @param servicePackage the package of the service
     * @param serviceClassName the class name of the service
     * @param inputMapping the input type mapping
     * @param outputMapping the output type mapping
     * @param streamingShape the streaming shape for the service
     * @param stepKind the kind of step (local or remote)
     * @param enabledTargets the set of generation targets to enable
     * @param executionMode the execution mode to use
     * @param restPath the REST path (optional)
     * @param grpcImplType the gRPC implementation type
     * @param grpcStubType the gRPC stub type
     * @param grpcClientName the gRPC client name for @GrpcClient annotation
     */
    public PipelineStepIR(String serviceName, String servicePackage, ClassName serviceClassName,
                         TypeMapping inputMapping, TypeMapping outputMapping,
                         StreamingShape streamingShape, StepKind stepKind,
                         Set<GenerationTarget> enabledTargets, ExecutionMode executionMode, String restPath, TypeName grpcImplType, TypeName grpcStubType, String grpcClientName) {
        this.serviceName = serviceName;
        this.servicePackage = servicePackage;
        this.serviceClassName = serviceClassName;
        this.inputMapping = inputMapping;
        this.outputMapping = outputMapping;
        this.streamingShape = streamingShape;
        this.stepKind = stepKind;
        this.enabledTargets = Collections.unmodifiableSet(new HashSet<>(enabledTargets)); // Defensive copy
        this.executionMode = executionMode;
        this.restPath = restPath;
        this.grpcImplType = grpcImplType;
        this.grpcStubType = grpcStubType;
        this.grpcClientName = grpcClientName;

        // Validate non-null invariants
        if (serviceName == null) throw new IllegalArgumentException("serviceName cannot be null");
        if (servicePackage == null) throw new IllegalArgumentException("servicePackage cannot be null");
        if (serviceClassName == null) throw new IllegalArgumentException("serviceClassName cannot be null");
        if (streamingShape == null) throw new IllegalArgumentException("streamingShape cannot be null");
        if (stepKind == null) throw new IllegalArgumentException("stepKind cannot be null");
        if (enabledTargets == null) throw new IllegalArgumentException("enabledTargets cannot be null");
        if (executionMode == null) throw new IllegalArgumentException("executionMode cannot be null");
    }

    /**
     * Gets the name of the service.
     *
     * @return the service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the package of the service.
     *
     * @return the service package
     */
    public String getServicePackage() {
        return servicePackage;
    }

    /**
     * Gets the ClassName of the service.
     *
     * @return the service class name
     */
    public ClassName getServiceClassName() {
        return serviceClassName;
    }

    /**
     * Gets the input type mapping for this service.
     *
     * @return the input type mapping
     */
    public TypeMapping getInputMapping() {
        return inputMapping;
    }

    /**
     * Gets the output type mapping for this service.
     *
     * @return the output type mapping
     */
    public TypeMapping getOutputMapping() {
        return outputMapping;
    }

    /**
     * Gets the streaming shape for this service.
     *
     * @return the streaming shape
     */
    public StreamingShape getStreamingShape() {
        return streamingShape;
    }

    /**
     * Gets the step kind for this service.
     *
     * @return the step kind
     */
    public StepKind getStepKind() {
        return stepKind;
    }

    /**
     * Gets the set of enabled generation targets.
     *
     * @return the enabled generation targets
     */
    public Set<GenerationTarget> getEnabledTargets() {
        return enabledTargets;
    }

    /**
     * Gets the execution mode for this service.
     *
     * @return the execution mode
     */
    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    /**
     * Gets the REST path for this service.
     *
     * @return the REST path, or null if not applicable
     */
    public String getRestPath() {
        return restPath;
    }

    /**
     * Gets the gRPC implementation type for this service.
     *
     * @return the gRPC implementation type
     */
    public TypeName getGrpcImplType() {
        return grpcImplType;
    }

    /**
     * Gets the gRPC stub type for this service.
     *
     * @return the gRPC stub type
     */
    public TypeName getGrpcStubType() {
        return grpcStubType;
    }

    /**
     * Gets the gRPC client name for this service.
     *
     * @return the gRPC client name
     */
    public String getGrpcClientName() {
        return grpcClientName;
    }

    public TypeName inboundGrpcParamType() {
        return inputMapping.getGrpcType();
    }

    /**
     * Builder class for creating PipelineStepIR instances.
     */
    public static class Builder {

        /**
         * Creates a new Builder instance.
         */
        public Builder() {
        }
        private String serviceName;
        private String servicePackage;
        private ClassName serviceClassName;
        private TypeMapping inputMapping;
        private TypeMapping outputMapping;
        private StreamingShape streamingShape;
        private StepKind stepKind;
        private Set<GenerationTarget> enabledTargets = new HashSet<>();
        private ExecutionMode executionMode;
        private String restPath;
        private TypeName grpcImplType;
        private TypeName grpcStubType;
        private String grpcClientName;

        /**
         * Sets the service name.
         *
         * @param serviceName the service name to set
         * @return this builder instance
         */
        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        /**
         * Sets the service package.
         *
         * @param servicePackage the service package to set
         * @return this builder instance
         */
        public Builder servicePackage(String servicePackage) {
            this.servicePackage = servicePackage;
            return this;
        }

        /**
         * Sets the service class name.
         *
         * @param serviceClassName the service class name to set
         * @return this builder instance
         */
        public Builder serviceClassName(ClassName serviceClassName) {
            this.serviceClassName = serviceClassName;
            return this;
        }

        /**
         * Sets the input type mapping.
         *
         * @param inputMapping the input type mapping to set
         * @return this builder instance
         */
        public Builder inputMapping(TypeMapping inputMapping) {
            this.inputMapping = inputMapping;
            return this;
        }

        /**
         * Sets the output type mapping.
         *
         * @param outputMapping the output type mapping to set
         * @return this builder instance
         */
        public Builder outputMapping(TypeMapping outputMapping) {
            this.outputMapping = outputMapping;
            return this;
        }

        /**
         * Sets the streaming shape.
         *
         * @param streamingShape the streaming shape to set
         * @return this builder instance
         */
        public Builder streamingShape(StreamingShape streamingShape) {
            this.streamingShape = streamingShape;
            return this;
        }

        /**
         * Sets the step kind.
         *
         * @param stepKind the step kind to set
         * @return this builder instance
         */
        public Builder stepKind(StepKind stepKind) {
            this.stepKind = stepKind;
            return this;
        }

        /**
         * Adds an enabled generation target.
         *
         * @param target the generation target to add
         * @return this builder instance
         */
        public Builder addEnabledTarget(GenerationTarget target) {
            this.enabledTargets.add(target);
            return this;
        }

        /**
         * Sets the enabled generation targets.
         *
         * @param enabledTargets the enabled generation targets to set
         * @return this builder instance
         */
        public Builder enabledTargets(Set<GenerationTarget> enabledTargets) {
            this.enabledTargets = new HashSet<>(enabledTargets);
            return this;
        }

        /**
         * Sets the execution mode.
         *
         * @param executionMode the execution mode to set
         * @return this builder instance
         */
        public Builder executionMode(ExecutionMode executionMode) {
            this.executionMode = executionMode;
            return this;
        }

        /**
         * Sets the REST path.
         *
         * @param restPath the REST path to set
         * @return this builder instance
         */
        public Builder restPath(String restPath) {
            this.restPath = restPath;
            return this;
        }

        /**
         * Sets the gRPC implementation type.
         *
         * @param grpcImplType the gRPC implementation type to set
         * @return this builder instance
         */
        public Builder grpcImplType(TypeName grpcImplType) {
            this.grpcImplType = grpcImplType;
            return this;
        }

        /**
         * Sets the gRPC stub type.
         *
         * @param grpcStubType the gRPC stub type to set
         * @return this builder instance
         */
        public Builder grpcStubType(TypeName grpcStubType) {
            this.grpcStubType = grpcStubType;
            return this;
        }

        /**
         * Sets the gRPC client name.
         *
         * @param grpcClientName the gRPC client name to set
         * @return this builder instance
         */
        public Builder grpcClientName(String grpcClientName) {
            this.grpcClientName = grpcClientName;
            return this;
        }

        /**
         * Builds the PipelineStepIR instance.
         *
         * @return the constructed PipelineStepIR instance
         */
        public PipelineStepIR build() {
            // Validate required fields are not null
            if (serviceName == null) throw new IllegalStateException("serviceName is required");
            if (servicePackage == null) throw new IllegalStateException("servicePackage is required");
            if (serviceClassName == null) throw new IllegalStateException("serviceClassName is required");
            if (streamingShape == null) throw new IllegalStateException("streamingShape is required");
            if (stepKind == null) throw new IllegalStateException("stepKind is required");
            if (executionMode == null) throw new IllegalStateException("executionMode is required");

            return new PipelineStepIR(serviceName, servicePackage, serviceClassName,
                                     inputMapping, outputMapping, streamingShape,
                                     stepKind, enabledTargets, executionMode, restPath, grpcImplType, grpcStubType, grpcClientName);
        }
    }
}