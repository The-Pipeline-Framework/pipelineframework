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
        Optional<TypeName> canonicalInputType = canonicalAwaitType(
            model.inboundDomainType(),
            transportInputType,
            context.pipelineBasePackage());
        Optional<TypeName> canonicalOutputType = canonicalAwaitType(
            model.outboundDomainType(),
            transportOutputType,
            context.pipelineBasePackage());
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
        GenerationContext context
    ) {
        if (!context.v3GeneratedDomainTypes() || context.pipelineBasePackage() == null) {
            return RepresentationBoundary.transportOnly(transportInputType, transportOutputType);
        }
        if (isExactPair(model.inboundDomainType(), transportInputType, context.pipelineBasePackage())
            && isExactPair(model.outboundDomainType(), transportOutputType, context.pipelineBasePackage())) {
            return new RepresentationBoundary(
                model.inboundDomainType(),
                model.outboundDomainType(),
                transportInputType,
                transportOutputType,
                Optional.of(ClassName.get(context.pipelineBasePackage() + ".domain", "PipelineDomainProtoAdapters")));
        }
        return RepresentationBoundary.transportOnly(transportInputType, transportOutputType);
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

    private static Optional<TypeName> canonicalAwaitType(TypeName modelType, TypeName transportType, String basePackage) {
        if (isExactPair(modelType, transportType, basePackage)) {
            return Optional.of(modelType);
        }
        if (transportType instanceof ClassName transport
            && transport.canonicalName().startsWith(basePackage + ".grpc.PipelineTypes.")) {
            return Optional.of(ClassName.get(basePackage + ".domain", transport.simpleName()));
        }
        return Optional.empty();
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
