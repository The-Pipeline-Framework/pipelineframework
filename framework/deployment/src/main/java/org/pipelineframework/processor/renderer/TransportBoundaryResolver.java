package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import java.io.IOException;
import java.util.Optional;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;

/**
 * Resolves a normalized v3 Java/protobuf boundary and retains the legacy generated-domain convention as a fallback.
 *
 * <p>Normalized record bindings use compiler-owned per-representation mappers. Existing
 * application-authored v3 domain/protobuf pairs retain their shared generated adapter, and
 * legacy callers retain the application-owned mapper path.</p>
 */
final class TransportBoundaryResolver {

    private TransportBoundaryResolver() {
    }

    static RepresentationBoundary resolve(
            PipelineStepModel model,
            GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes,
            GenerationContext context) throws IOException {
        TypeName transportInputType = grpcTypes.grpcParameterType();
        TypeName transportOutputType = grpcTypes.grpcReturnType();
        CanonicalTransportBindingPair normalized = CanonicalTransportBindingResolver.resolveAndEnsure(
            context, model, PipelineTransport.GRPC);
        Optional<CanonicalTransportTypeBinding> normalizedInput = normalized.input();
        Optional<CanonicalTransportTypeBinding> normalizedOutput = normalized.output();
        if (normalizedInput.isPresent() && normalizedOutput.isPresent()) {
            return new RepresentationBoundary(
                model.inboundDomainType(),
                model.outboundDomainType(),
                transportInputType,
                transportOutputType,
                Optional.of(normalizedInput.orElseThrow().grpcMapperType()),
                Optional.of(normalizedOutput.orElseThrow().grpcMapperType()));
        }
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
                Optional.of(ClassName.get(context.pipelineBasePackage() + ".domain", "PipelineDomainProtoAdapters")),
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
                Optional.of(ClassName.get(pipelineBasePackage + ".domain", "PipelineDomainProtoAdapters")),
                Optional.of(ClassName.get(pipelineBasePackage + ".domain", "PipelineDomainProtoAdapters")));
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

    /**
     * Explicitly separates the public canonical contract from the protobuf transport contract.
     * Non-v3 callers retain the existing transport-only shape without special mapper resolution.
     */
    record RepresentationBoundary(
        TypeName stepInputType,
        TypeName stepOutputType,
        TypeName transportInputType,
        TypeName transportOutputType,
        Optional<ClassName> inputAdapter,
        Optional<ClassName> outputAdapter
    ) {
        static RepresentationBoundary transportOnly(TypeName inputType, TypeName outputType) {
            return new RepresentationBoundary(
                inputType, outputType, inputType, outputType, Optional.empty(), Optional.empty());
        }

        boolean convertsAtBoundary() {
            return inputAdapter.isPresent() && outputAdapter.isPresent();
        }

        ClassName inputAdapterOrThrow() {
            return inputAdapter.orElseThrow(() -> new IllegalStateException(
                "A generated representation boundary requires an input protobuf adapter class"));
        }

        ClassName outputAdapterOrThrow() {
            return outputAdapter.orElseThrow(() -> new IllegalStateException(
                "A generated representation boundary requires an output protobuf adapter class"));
        }
    }
}
