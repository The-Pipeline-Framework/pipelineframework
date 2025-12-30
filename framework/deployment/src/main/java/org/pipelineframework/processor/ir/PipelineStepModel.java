package org.pipelineframework.processor.ir;

import java.util.HashSet;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

/**
 * Contains only semantic information derived from @PipelineStep annotations. This class captures all the essential
 * information needed to generate pipeline artifacts.
 *
 * @param serviceName Gets the name of the service.
 * @param servicePackage Gets the package of the service.
 * @param serviceClassName Gets the ClassName of the service.
 * @param inputMapping Gets the input type mapping for this service. Directional type mappings Domain -> gRPC
 * @param outputMapping Gets the output type mapping for this service. Domain -> gRPC
 * @param streamingShape Gets the streaming shape for this service. Semantic configuration
 * @param enabledTargets Gets the set of enabled generation targets.
 * @param executionMode Gets the execution mode for this service.
 */
public record PipelineStepModel(
        String serviceName,
        String servicePackage,
        ClassName serviceClassName,
        TypeMapping inputMapping,
        TypeMapping outputMapping,
        StreamingShape streamingShape,
        Set<GenerationTarget> enabledTargets,
        ExecutionMode executionMode
) {
    /**
     * Creates a new PipelineStepModel instance.
     *
     * @param serviceName the name of the service
     * @param servicePackage the package of the service
     * @param serviceClassName the class name of the service
     * @param inputMapping the input type mapping
     * @param outputMapping the output type mapping
     * @param streamingShape the streaming shape for the service
     * @param enabledTargets the set of generation targets to enable
     * @param executionMode the execution mode to use
     */
    @SuppressWarnings("ConstantValue")
    public PipelineStepModel(String serviceName,
            String servicePackage,
            ClassName serviceClassName,
            TypeMapping inputMapping,
            TypeMapping outputMapping,
            StreamingShape streamingShape,
            Set<GenerationTarget> enabledTargets,
            ExecutionMode executionMode) {
        this.serviceName = serviceName;
        this.servicePackage = servicePackage;
        this.serviceClassName = serviceClassName;
        this.inputMapping = inputMapping;
        this.outputMapping = outputMapping;
        this.streamingShape = streamingShape;
        this.enabledTargets = Set.copyOf(enabledTargets); // Defensive copy
        this.executionMode = executionMode;

        // Validate non-null invariants
        if (serviceName == null)
            throw new IllegalArgumentException("serviceName cannot be null");
        if (servicePackage == null)
            throw new IllegalArgumentException("servicePackage cannot be null");
        if (serviceClassName == null)
            throw new IllegalArgumentException("serviceClassName cannot be null");
        if (streamingShape == null)
            throw new IllegalArgumentException("streamingShape cannot be null");
        if (enabledTargets == null)
            throw new IllegalArgumentException("enabledTargets cannot be null");
        if (executionMode == null)
            throw new IllegalArgumentException("executionMode cannot be null");
    }

    /**
     * Gets the inbound domain type.
     * @return the inbound domain type
     */
    public TypeName inboundDomainType() {
        return inputMapping.domainType();
    }

    /**
     * Gets the outbound domain type.
     * @return the outbound domain type
     */
    public TypeName outboundDomainType() {
        return outputMapping.domainType();
    }

    /**
     * Builder class for creating PipelineStepModel instances.
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
        private Set<GenerationTarget> enabledTargets = new HashSet<>();
        private ExecutionMode executionMode;

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
         * Builds the PipelineStepModel instance.
         *
         * @return the constructed PipelineStepModel instance
         */
        public PipelineStepModel build() {
            // Validate required fields are not null
            if (serviceName == null)
                throw new IllegalStateException("serviceName is required");
            if (servicePackage == null)
                throw new IllegalStateException("servicePackage is required");
            if (serviceClassName == null)
                throw new IllegalStateException("serviceClassName is required");
            if (streamingShape == null)
                throw new IllegalStateException("streamingShape is required");
            if (executionMode == null)
                throw new IllegalStateException("executionMode is required");

            return new PipelineStepModel(serviceName,
                    servicePackage,
                    serviceClassName,
                    inputMapping,
                    outputMapping,
                    streamingShape,
                    enabledTargets,
                    executionMode);
        }
    }
}