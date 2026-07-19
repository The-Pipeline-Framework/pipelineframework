package org.pipelineframework.processor.renderer;

import java.nio.file.Path;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineTransport;

/**
 * Context for code generation operations, containing processing environment and output directory information.
 *
 * @param processingEnv Gets the processing environment.
 * @param outputDir Gets the base directory for generated sources for a specific role.
 * @param role Gets the deployment role for the artifact being rendered.
 * @param enabledAspects Gets the set of enabled pipeline aspect names.
 * @param cacheKeyGenerator Gets the optional cache key generator class name for generated cache annotations.
 * @param descriptorSet Gets the optional protobuf descriptor set for gRPC type resolution.
 * @param transportMode Gets the resolved pipeline transport mode when available.
 * @param pipelineBasePackage Gets the resolved pipeline base package when available.
 * @param stepOrder Gets the zero-based resolved step order when rendering an ordered pipeline step.
 * @param v3GeneratedDomainTypes Whether the current pipeline has generated v3 Java domain types available.
 */
public record GenerationContext(ProcessingEnvironment processingEnv, Path outputDir, DeploymentRole role,
                                Set<String> enabledAspects, ClassName cacheKeyGenerator,
                                DescriptorProtos.FileDescriptorSet descriptorSet,
                                PipelineTransport transportMode,
                                String pipelineBasePackage,
                                Integer stepOrder,
                                boolean v3GeneratedDomainTypes) {
    /**
     * Creates a new GenerationContext instance.
     */
    public GenerationContext {
        enabledAspects = enabledAspects == null ? Set.of() : Set.copyOf(enabledAspects);
    }

    public GenerationContext(ProcessingEnvironment processingEnv, Path outputDir, DeploymentRole role,
                             Set<String> enabledAspects, ClassName cacheKeyGenerator,
                             DescriptorProtos.FileDescriptorSet descriptorSet,
                             PipelineTransport transportMode,
                             String pipelineBasePackage) {
        this(processingEnv,
            outputDir,
            role,
            enabledAspects,
            cacheKeyGenerator,
            descriptorSet,
            transportMode,
            pipelineBasePackage,
            null,
            false);
    }

    public GenerationContext(ProcessingEnvironment processingEnv, Path outputDir, DeploymentRole role,
                             Set<String> enabledAspects, ClassName cacheKeyGenerator,
                             DescriptorProtos.FileDescriptorSet descriptorSet,
                             PipelineTransport transportMode,
                             String pipelineBasePackage,
                             Integer stepOrder) {
        this(processingEnv,
            outputDir,
            role,
            enabledAspects,
            cacheKeyGenerator,
            descriptorSet,
            transportMode,
            pipelineBasePackage,
            stepOrder,
            false);
    }

    public GenerationContext(
            ProcessingEnvironment processingEnv,
            Path outputDir,
            DeploymentRole role,
            Set<String> enabledAspects,
            ClassName cacheKeyGenerator,
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        this(processingEnv, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet, null, null, null, false);
    }
}
