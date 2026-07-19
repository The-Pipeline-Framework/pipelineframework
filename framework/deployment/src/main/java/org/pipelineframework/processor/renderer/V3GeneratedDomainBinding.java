package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
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

    static boolean applies(
            PipelineStepModel model,
            GrpcJavaTypeResolver.GrpcJavaTypes grpcTypes,
            GenerationContext context) {
        if (!context.v3GeneratedDomainTypes() || context.pipelineBasePackage() == null
                || model.streamingShape() != org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY) {
            return false;
        }
        return isExactPair(model.inboundDomainType(), grpcTypes.grpcParameterType(), context.pipelineBasePackage())
            && isExactPair(model.outboundDomainType(), grpcTypes.grpcReturnType(), context.pipelineBasePackage());
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
}
