package org.pipelineframework.processor.phase;

import java.util.HashSet;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

/**
 * Request context for per-target generation.
 *
 * <p>All fields are immutable except {@code generatedSideEffectBeans}, which is intentionally shared and mutable
 * to support de-duplication across target generators during a single phase execution.</p>
 */
public record GenerationRequest(
    PipelineCompilationContext ctx,
    PipelineStepModel model,
    GrpcBinding grpcBinding,
    RestBinding restBinding,
    LocalBinding localBinding,
    Set<String> generatedSideEffectBeans,
    DescriptorProtos.FileDescriptorSet descriptorSet,
    ClassName cacheKeyGenerator,
    RoleMetadataGenerator roleMetadataGenerator,
    Set<String> enabledAspects
) {
    public GenerationRequest {
        generatedSideEffectBeans = generatedSideEffectBeans == null ? new HashSet<>() : generatedSideEffectBeans;
        enabledAspects = enabledAspects == null ? Set.of() : Set.copyOf(enabledAspects);
    }
}
