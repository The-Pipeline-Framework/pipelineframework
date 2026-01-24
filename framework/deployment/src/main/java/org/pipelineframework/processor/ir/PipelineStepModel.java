package org.pipelineframework.processor.ir;

import java.util.HashSet;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Contains only semantic information derived from @PipelineStep annotations. This class captures all the essential
 * information needed to generate pipeline artifacts.
 *
 * @param serviceName Gets the name of the service.
 * @param generatedName Gets the generated class name base for the service.
 * @param servicePackage Gets the package of the service.
 * @param serviceClassName Gets the ClassName of the service.
 * @param inputMapping Gets the input type mapping for this service. Directional type mappings Domain -> gRPC
 * @param outputMapping Gets the output type mapping for this service. Domain -> gRPC
 * @param streamingShape Gets the streaming shape for this service. Semantic configuration
 * @param enabledTargets Gets the set of enabled generation targets.
 * @param executionMode Gets the execution mode for this service.
 * @param deploymentRole Gets the deployment role for the service implementation.
 * @param sideEffect Gets whether the step is a synthetic side-effect observer.
 * @param cacheKeyGenerator Gets the cache key generator override class for this step, if any.
 * @param orderingRequirement Gets the ordering requirement for the generated client step.
 * @param threadSafety Gets the thread safety declaration for the generated client step.
 */
public record PipelineStepModel(
        String serviceName,
        String generatedName,
        String servicePackage,
        ClassName serviceClassName,
        TypeMapping inputMapping,
        TypeMapping outputMapping,
        StreamingShape streamingShape,
        Set<GenerationTarget> enabledTargets,
        ExecutionMode executionMode,
        DeploymentRole deploymentRole,
        boolean sideEffect,
        ClassName cacheKeyGenerator,
        OrderingRequirement orderingRequirement,
        ThreadSafety threadSafety
) {
    /**
         * Creates a new PipelineStepModel with the supplied service identity, type mappings and generation configuration.
         *
         * @param serviceName      the service name; must not be null
         * @param servicePackage   the service package; must not be null
         * @param serviceClassName the service class name; must not be null
         * @param inputMapping     the input domain→gRPC type mapping, or {@code null} if not applicable
         * @param outputMapping    the output domain→gRPC type mapping, or {@code null} if not applicable
         * @param streamingShape   the streaming shape configuration; must not be null
         * @param enabledTargets   the set of enabled generation targets; must not be null
         * @param executionMode    the execution mode for the service; must not be null
         * @param deploymentRole   the deployment role for the service implementation; must not be null
     * @param cacheKeyGenerator the cache key generator override for this step; may be null
     * @param orderingRequirement the ordering requirement for the generated client step; may be null
     * @param threadSafety the thread safety declaration for the generated client step; may be null
     * @throws IllegalArgumentException if any parameter documented as 'must not be null' is null
     */
    @SuppressWarnings("ConstantValue")
    public PipelineStepModel(String serviceName,
            String generatedName,
            String servicePackage,
            ClassName serviceClassName,
            TypeMapping inputMapping,
            TypeMapping outputMapping,
            StreamingShape streamingShape,
            Set<GenerationTarget> enabledTargets,
            ExecutionMode executionMode,
            DeploymentRole deploymentRole,
            boolean sideEffect,
            ClassName cacheKeyGenerator,
            OrderingRequirement orderingRequirement,
            ThreadSafety threadSafety) {
        // Validate non-null invariants
        if (serviceName == null)
            throw new IllegalArgumentException("serviceName cannot be null");
        if (generatedName == null)
            throw new IllegalArgumentException("generatedName cannot be null");
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
        if (deploymentRole == null)
            throw new IllegalArgumentException("deploymentRole cannot be null");

        this.serviceName = serviceName;
        this.generatedName = generatedName;
        this.servicePackage = servicePackage;
        this.serviceClassName = serviceClassName;
        this.inputMapping = inputMapping != null ? inputMapping : new TypeMapping(null, null, false);
        this.outputMapping = outputMapping != null ? outputMapping : new TypeMapping(null, null, false);
        this.streamingShape = streamingShape;
        this.enabledTargets = Set.copyOf(enabledTargets); // Defensive copy
        this.executionMode = executionMode;
        this.deploymentRole = deploymentRole;
        this.sideEffect = sideEffect;
        this.cacheKeyGenerator = cacheKeyGenerator;
        this.orderingRequirement = orderingRequirement != null ? orderingRequirement : OrderingRequirement.RELAXED;
        this.threadSafety = threadSafety != null ? threadSafety : ThreadSafety.SAFE;
    }

    /**
     * Create a pipeline step model with default ordering and thread-safety hints.
     *
     * @param serviceName service name from the step class
     * @param generatedName generated service name for adapters
     * @param servicePackage base service package
     * @param serviceClassName service class name
     * @param inputMapping input type mapping
     * @param outputMapping output type mapping
     * @param streamingShape streaming shape for the step
     * @param enabledTargets generation targets to render
     * @param executionMode execution mode
     * @param deploymentRole deployment role
     * @param sideEffect whether this step is a side-effect plugin
     * @param cacheKeyGenerator optional cache key generator class
     */
    public PipelineStepModel(String serviceName,
            String generatedName,
            String servicePackage,
            ClassName serviceClassName,
            TypeMapping inputMapping,
            TypeMapping outputMapping,
            StreamingShape streamingShape,
            Set<GenerationTarget> enabledTargets,
            ExecutionMode executionMode,
            DeploymentRole deploymentRole,
            boolean sideEffect,
            ClassName cacheKeyGenerator) {
        this(serviceName,
            generatedName,
            servicePackage,
            serviceClassName,
            inputMapping,
            outputMapping,
            streamingShape,
            enabledTargets,
            executionMode,
            deploymentRole,
            sideEffect,
            cacheKeyGenerator,
            OrderingRequirement.RELAXED,
            ThreadSafety.SAFE);
    }

    /**
     * The inbound domain type for this pipeline step.
     *
     * @return the domain `TypeName` used as the service's input
     */
    public TypeName inboundDomainType() {
        return inputMapping.domainType();
    }

    /**
     * Obtain the domain type used for outbound mapping.
     *
     * @return a TypeName representing the outbound domain type
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
        private String generatedName;
        private String servicePackage;
        private ClassName serviceClassName;
        private TypeMapping inputMapping = new TypeMapping(null, null, false);
        private TypeMapping outputMapping = new TypeMapping(null, null, false);
        private StreamingShape streamingShape;
        private Set<GenerationTarget> enabledTargets = new HashSet<>();
        private ExecutionMode executionMode;
        private DeploymentRole deploymentRole = DeploymentRole.PIPELINE_SERVER;
        private boolean sideEffect;
        private ClassName cacheKeyGenerator;
        private OrderingRequirement orderingRequirement = OrderingRequirement.RELAXED;
        private ThreadSafety threadSafety = ThreadSafety.SAFE;

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
         * Sets the generated class name base for the service.
         *
         * @param generatedName the generated class name base to set
         * @return this builder instance
         */
        public Builder generatedName(String generatedName) {
            this.generatedName = generatedName;
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
         * Sets the service class's ClassName used to identify the service implementation.
         *
         * @param serviceClassName the ClassName representing the service class
         * @return this builder instance
         */
        public Builder serviceClassName(ClassName serviceClassName) {
            this.serviceClassName = serviceClassName;
            return this;
        }

        /**
         * Set the mapping used to convert the service's inbound domain type to its gRPC representation.
         *
         * @param inputMapping mapping describing how the service input domain type is translated to transport types
         * @return this builder instance
         */
        public Builder inputMapping(TypeMapping inputMapping) {
            this.inputMapping = inputMapping;
            return this;
        }

        /**
         * Set the output type mapping used for the service's outbound domain to gRPC mapping.
         *
         * @param outputMapping mapping describing how domain output types map to gRPC types
         * @return this builder instance
         */
        public Builder outputMapping(TypeMapping outputMapping) {
            this.outputMapping = outputMapping;
            return this;
        }

        /**
         * Set the streaming shape for the pipeline step under construction.
         *
         * @param streamingShape the streaming shape configuration for the service
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
         * Replace the builder's enabled generation targets with a defensive copy of the given set.
         *
         * @param enabledTargets the set of generation targets to enable; a defensive copy is stored
         * @return this builder instance
         */
        public Builder enabledTargets(Set<GenerationTarget> enabledTargets) {
            this.enabledTargets = new HashSet<>(enabledTargets);
            return this;
        }

        /**
         * Set the execution mode for the pipeline step being built.
         *
         * @param executionMode the execution mode to apply
         * @return this builder instance
         */
        public Builder executionMode(ExecutionMode executionMode) {
            this.executionMode = executionMode;
            return this;
        }

        /**
         * Set the deployment role for the service implementation.
         *
         * @param deploymentRole the deployment role to apply
         * @return this builder instance
         */
        public Builder deploymentRole(DeploymentRole deploymentRole) {
            this.deploymentRole = deploymentRole;
            return this;
        }

        /**
         * Marks the step as a synthetic side-effect observer.
         *
         * @param sideEffect whether the step is a side-effect observer
         * @return this builder instance
         */
        public Builder sideEffect(boolean sideEffect) {
            this.sideEffect = sideEffect;
            return this;
        }

        /**
         * Sets the cache key generator override for this step.
         *
         * @param cacheKeyGenerator the cache key generator class to use; may be null
         * @return this builder instance
         */
        public Builder cacheKeyGenerator(ClassName cacheKeyGenerator) {
            this.cacheKeyGenerator = cacheKeyGenerator;
            return this;
        }

        /**
         * Sets the ordering requirement for the generated client step.
         *
         * @param orderingRequirement the ordering requirement to apply
         * @return this builder instance
         */
        public Builder orderingRequirement(OrderingRequirement orderingRequirement) {
            this.orderingRequirement = orderingRequirement;
            return this;
        }

        /**
         * Sets the thread safety declaration for the generated client step.
         *
         * @param threadSafety the thread safety declaration to apply
         * @return this builder instance
         */
        public Builder threadSafety(ThreadSafety threadSafety) {
            this.threadSafety = threadSafety;
            return this;
        }

        /**
         * Create a PipelineStepModel populated from the builder's current state.
         *
         * @return a PipelineStepModel populated with the builder's state
         * @throws IllegalStateException if any required property is not set — specifically when
         *                               serviceName, servicePackage, serviceClassName,
         *                               streamingShape, or executionMode is null
         */
        public PipelineStepModel build() {
            // Validate required fields are not null
            if (serviceName == null)
                throw new IllegalStateException("serviceName is required");
            if (generatedName == null) {
                generatedName = serviceName;
            }
            if (servicePackage == null)
                throw new IllegalStateException("servicePackage is required");
            if (serviceClassName == null)
                throw new IllegalStateException("serviceClassName is required");
            if (streamingShape == null)
                throw new IllegalStateException("streamingShape is required");
            if (executionMode == null)
                throw new IllegalStateException("executionMode is required");
            if (deploymentRole == null)
                throw new IllegalStateException("deploymentRole is required");

            return new PipelineStepModel(serviceName,
                    generatedName,
                    servicePackage,
                    serviceClassName,
                    inputMapping,
                    outputMapping,
                    streamingShape,
                    enabledTargets,
                    executionMode,
                    deploymentRole,
                    sideEffect,
                    cacheKeyGenerator,
                    orderingRequirement,
                    threadSafety);
        }
    }
}
