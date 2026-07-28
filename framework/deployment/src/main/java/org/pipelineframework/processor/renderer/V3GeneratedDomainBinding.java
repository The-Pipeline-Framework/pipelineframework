package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import java.util.Optional;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;

/**
 * Identifies the deliberately narrow v3 generated-domain/protobuf binding.
 *
 * <p>This is not mapper selection: it applies only when both sides have the
 * exact names emitted by the v3 target renderers. All other boundaries retain
 * the normal application-owned mapper path.</p>
 */
final class V3GeneratedDomainBinding {

    private V3GeneratedDomainBinding() {
    }

    static RepresentationBoundary resolve(
            PipelineStepModel model,
            GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes,
            GenerationContext context) {
        TypeName transportInputType = grpcTypes.grpcParameterType();
        TypeName transportOutputType = grpcTypes.grpcReturnType();
        if (!context.v3GeneratedDomainTypes() || context.pipelineBasePackage() == null) {
            return RepresentationBoundary.transportOnly(transportInputType, transportOutputType);
        }
        Optional<TypeName> canonicalInputType = isExactPair(
            model.inboundDomainType(), transportInputType, context.pipelineBasePackage())
                ? Optional.of(model.inboundDomainType())
                : Optional.empty();
        Optional<TypeName> canonicalOutputType = isExactPair(
            model.outboundDomainType(), transportOutputType, context.pipelineBasePackage())
                ? Optional.of(model.outboundDomainType())
                : Optional.empty();
        if (canonicalInputType.isPresent() && canonicalOutputType.isPresent()) {
            return new RepresentationBoundary(
                canonicalInputType.orElseThrow(),
                canonicalOutputType.orElseThrow(),
                transportInputType,
                transportOutputType,
                Optional.of(ClassName.get(context.pipelineBasePackage() + ".domain", "PipelineDomainProtoAdapters")));
        }
        return RepresentationBoundary.transportOnly(transportInputType, transportOutputType);
    }

    static RepresentationBoundary resolveAwait(
        PipelineStepModel model,
        TypeName transportInputType,
        TypeName transportOutputType,
        String pipelineBasePackage,
        boolean v3GeneratedDomainTypes
    ) {
        if (!v3GeneratedDomainTypes || pipelineBasePackage == null) {
            return RepresentationBoundary.transportOnly(transportInputType, transportOutputType);
        }
        if (isExactPair(model.inboundDomainType(), transportInputType, pipelineBasePackage)
            && isExactPair(model.outboundDomainType(), transportOutputType, pipelineBasePackage)) {
            return new RepresentationBoundary(
                model.inboundDomainType(),
                model.outboundDomainType(),
                transportInputType,
                transportOutputType,
                Optional.of(ClassName.get(pipelineBasePackage + ".domain", "PipelineDomainProtoAdapters")));
        }
        return RepresentationBoundary.transportOnly(transportInputType, transportOutputType);
    }

    /**
     * Determines whether a model carries the generated v3 canonical contract names.
     *
     * <p>Some aggregate builds compile child modules through a legacy processor
     * lifecycle that does not retain the template dialect in the generation
     * context. The normalized domain package remains a stable fallback signal.</p>
     */
    static boolean hasGeneratedCanonicalContracts(PipelineStepModel model, String basePackage) {
        return isGeneratedCanonicalDomainType(model.inboundDomainType(), basePackage)
            && isGeneratedCanonicalDomainType(model.outboundDomainType(), basePackage);
    }

    private static boolean isGeneratedCanonicalDomainType(TypeName type, String basePackage) {
        return type instanceof ClassName className
            && basePackage != null
            && className.packageName().equals(basePackage + ".domain");
    }

    private static boolean isExactPair(TypeName domainType, TypeName protoType, String basePackage) {
        if (!(domainType instanceof ClassName domain) || !(protoType instanceof ClassName proto)) {
            return false;
        }
        String domainPrefix = basePackage + ".domain.";
        String protoPrefix = basePackage + ".grpc.PipelineTypes.";
        return domain.canonicalName().startsWith(domainPrefix)
            && proto.canonicalName().startsWith(protoPrefix)
            && domain.simpleName().equals(proto.simpleName());
    }

    /**
     * Explicitly separates the public canonical contract from the protobuf transport contract.
     * Non-v3 callers retain the existing transport-only shape without special mapper resolution.
     */
    record RepresentationBoundary(
        TypeName stepInputType,
        TypeName stepOutputType,
        TypeName transportInputType,
        TypeName transportOutputType,
        Optional<ClassName> adapters
    ) {
        static RepresentationBoundary transportOnly(TypeName inputType, TypeName outputType) {
            return new RepresentationBoundary(inputType, outputType, inputType, outputType, Optional.empty());
        }

        boolean convertsAtBoundary() {
            return adapters.isPresent();
        }

        ClassName adaptersOrThrow() {
            return adapters.orElseThrow(() -> new IllegalStateException(
                "A generated representation boundary requires a protobuf adapter class"));
        }
    }
}
