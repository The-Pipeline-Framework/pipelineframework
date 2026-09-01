package org.pipelineframework.processor.renderer;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;

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
 * @param canonicalTypeModel Gets the normalized v3 type model when the compilation phase already carries it.
 */
public record GenerationContext(ProcessingEnvironment processingEnv, Path outputDir, DeploymentRole role,
                                Set<String> enabledAspects, ClassName cacheKeyGenerator,
                                DescriptorProtos.FileDescriptorSet descriptorSet,
                                PipelineTransport transportMode,
                                String pipelineBasePackage,
                                Integer stepOrder,
                                boolean v3GeneratedDomainTypes,
                                Optional<PipelineTemplateTypeModel> canonicalTypeModel) {
    /**
     * Creates a new GenerationContext instance.
     */
    public GenerationContext {
        enabledAspects = enabledAspects == null ? Set.of() : Set.copyOf(enabledAspects);
        canonicalTypeModel = canonicalTypeModel == null ? Optional.empty() : canonicalTypeModel;
    }

    /** Backward-compatible constructor for generation call sites without the normalized v3 type model. */
    public GenerationContext(ProcessingEnvironment processingEnv, Path outputDir, DeploymentRole role,
                             Set<String> enabledAspects, ClassName cacheKeyGenerator,
                             DescriptorProtos.FileDescriptorSet descriptorSet,
                             PipelineTransport transportMode, String pipelineBasePackage,
                             Integer stepOrder, boolean v3GeneratedDomainTypes) {
        this(processingEnv, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet,
            transportMode, pipelineBasePackage, stepOrder, v3GeneratedDomainTypes, Optional.empty());
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
            false,
            Optional.empty());
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
            false,
            Optional.empty());
    }

    public GenerationContext(
            ProcessingEnvironment processingEnv,
            Path outputDir,
            DeploymentRole role,
            Set<String> enabledAspects,
            ClassName cacheKeyGenerator,
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        this(processingEnv, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet,
            null, null, null, false, Optional.empty());
    }

}
