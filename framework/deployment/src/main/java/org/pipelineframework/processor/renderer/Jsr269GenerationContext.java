package org.pipelineframework.processor.renderer;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.processor.Jsr269CompilerServices;
import org.pipelineframework.processor.Jsr269PipelineCompilerDiagnostics;
import org.pipelineframework.processor.PipelineCompilerDiagnostics;
import org.pipelineframework.processor.PipelineCompilerOptions;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.PipelineTransport;

/** JSR-269 host adapter for assembling renderer contexts from compiler services. */
public final class Jsr269GenerationContext {
    private Jsr269GenerationContext() {
    }

    public static GenerationContext create(
        ProcessingEnvironment environment, Path outputDir, DeploymentRole role, Set<String> enabledAspects,
        ClassName cacheKeyGenerator, DescriptorProtos.FileDescriptorSet descriptorSet,
        PipelineTransport transportMode, String pipelineBasePackage, Integer stepOrder,
        boolean v3GeneratedDomainTypes, Optional<PipelineTemplateTypeModel> canonicalTypeModel
    ) {
        PipelineCompilerOptions options = new PipelineCompilerOptions(
            environment == null || environment.getOptions() == null ? Map.of() : environment.getOptions());
        PipelineCompilerDiagnostics diagnostics = environment == null || environment.getMessager() == null
            ? (severity, message) -> { }
            : new Jsr269PipelineCompilerDiagnostics(environment.getMessager());
        return new GenerationContext(Jsr269CompilerServices.from(environment), options, diagnostics,
            outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet, transportMode,
            pipelineBasePackage, stepOrder, v3GeneratedDomainTypes, canonicalTypeModel);
    }

    public static GenerationContext create(
        ProcessingEnvironment environment, Path outputDir, DeploymentRole role, Set<String> enabledAspects,
        ClassName cacheKeyGenerator, DescriptorProtos.FileDescriptorSet descriptorSet,
        PipelineTransport transportMode, String pipelineBasePackage, Integer stepOrder,
        boolean v3GeneratedDomainTypes
    ) {
        return create(environment, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet,
            transportMode, pipelineBasePackage, stepOrder, v3GeneratedDomainTypes, Optional.empty());
    }

    public static GenerationContext create(
        ProcessingEnvironment environment, Path outputDir, DeploymentRole role, Set<String> enabledAspects,
        ClassName cacheKeyGenerator, DescriptorProtos.FileDescriptorSet descriptorSet,
        PipelineTransport transportMode, String pipelineBasePackage, Integer stepOrder
    ) {
        return create(environment, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet,
            transportMode, pipelineBasePackage, stepOrder, false, Optional.empty());
    }

    public static GenerationContext create(
        ProcessingEnvironment environment, Path outputDir, DeploymentRole role, Set<String> enabledAspects,
        ClassName cacheKeyGenerator, DescriptorProtos.FileDescriptorSet descriptorSet,
        PipelineTransport transportMode, String pipelineBasePackage
    ) {
        return create(environment, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet,
            transportMode, pipelineBasePackage, null, false, Optional.empty());
    }

    public static GenerationContext create(
        ProcessingEnvironment environment, Path outputDir, DeploymentRole role, Set<String> enabledAspects,
        ClassName cacheKeyGenerator, DescriptorProtos.FileDescriptorSet descriptorSet
    ) {
        return create(environment, outputDir, role, enabledAspects, cacheKeyGenerator, descriptorSet,
            null, null, null, false, Optional.empty());
    }
}
