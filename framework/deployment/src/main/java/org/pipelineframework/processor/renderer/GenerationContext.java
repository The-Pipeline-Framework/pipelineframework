package org.pipelineframework.processor.renderer;

import java.nio.file.Path;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.ir.DeploymentRole;

/**
 * Context for code generation operations, containing processing environment and output directory information.
 *
 * @param processingEnv Gets the processing environment.
 * @param outputDir Gets the base directory for generated sources for a specific role.
 * @param role Gets the deployment role for the artifact being rendered.
 * @param enabledAspects Gets the set of enabled pipeline aspect names.
 * @param cacheKeyGenerator Gets the optional cache key generator class name for generated cache annotations.
 * @param descriptorSet Gets the optional protobuf descriptor set for gRPC type resolution.
 */
public record GenerationContext(ProcessingEnvironment processingEnv, Path outputDir, DeploymentRole role,
                                Set<String> enabledAspects, ClassName cacheKeyGenerator,
                                DescriptorProtos.FileDescriptorSet descriptorSet) {
    /**
     * Creates a new GenerationContext instance.
     */
    public GenerationContext {
        enabledAspects = enabledAspects == null ? Set.of() : Set.copyOf(enabledAspects);
    }
}
