package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.*;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;
import org.pipelineframework.processor.util.OrchestratorGrpcBindingResolver;
import org.pipelineframework.processor.util.OrchestratorRpcConstants;

/**
 * Generates a gRPC ingest client for orchestrator services.
 */
public class OrchestratorIngestClientRenderer {

    private static final String CLIENT_CLASS = "OrchestratorIngestClient";
    private static final String ORCHESTRATOR_INGEST_METHOD = OrchestratorRpcConstants.INGEST_METHOD;
    private static final String ORCHESTRATOR_SUBSCRIBE_METHOD = OrchestratorRpcConstants.SUBSCRIBE_METHOD;

    /**
     * Render the ingest client for the orchestrator binding.
     *
     * @param binding orchestrator binding
     * @param ctx generation context
     * @throws IOException when writing the output fails
     */
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.descriptorSet();
        if (descriptorSet == null) {
            throw new IllegalStateException("No protobuf descriptor set available for orchestrator ingest client.");
        }

        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        var ingestBinding = safeResolveBinding(binding, descriptorSet, ctx, ORCHESTRATOR_INGEST_METHOD, true, true);
        if (ingestBinding == null) {
            return;
        }

        GrpcJavaTypeResolver typeResolver = new GrpcJavaTypeResolver();
        var ingestTypes = typeResolver.resolve(ingestBinding, ctx.processingEnv().getMessager());
        if (ingestTypes == null) {
            if (ctx.processingEnv() != null && ctx.processingEnv().getMessager() != null) {
                ctx.processingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Skipping orchestrator ingest client generation: could not resolve gRPC types.");
            }
            return;
        }

        ClassName stubType = ingestTypes.stub();
        ClassName inputType = ingestTypes.grpcParameterType();
        ClassName outputType = ingestTypes.grpcReturnType();
        if (stubType == null || inputType == null || outputType == null) {
            throw new IllegalStateException("Failed to resolve orchestrator ingest client types from descriptors.");
        }

        ClassName subscribeInputType = resolveSubscribeInputType(binding, descriptorSet, ctx, typeResolver, outputType);
        if (subscribeInputType == null) {
            return;
        }

        FieldSpec grpcClientField = FieldSpec.builder(stubType, "grpcClient", Modifier.PRIVATE)
            .addAnnotation(AnnotationSpec.builder(GrpcClient.class)
                .addMember("value", "$S", "orchestrator")
                .build())
            .build();

        MethodSpec ingestMethod = MethodSpec.methodBuilder("ingest")
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
            .addParameter(ParameterizedTypeName.get(ClassName.get(Multi.class), inputType), "input")
            .addStatement("return grpcClient.ingest(input)")
            .build();

        MethodSpec subscribeMethod = MethodSpec.methodBuilder("subscribe")
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(ClassName.get(Multi.class), outputType))
            .addStatement("return grpcClient.subscribe($T.getDefaultInstance())", subscribeInputType)
            .build();

        TypeSpec client = TypeSpec.classBuilder(CLIENT_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "Dependent"))
            .addAnnotation(ClassName.get(Unremovable.class))
            .addField(grpcClientField)
            .addMethod(ingestMethod)
            .addMethod(subscribeMethod)
            .build();

        JavaFile.builder(binding.basePackage() + ".orchestrator.client", client)
            .build()
            .writeTo(ctx.processingEnv().getFiler());
    }

    private org.pipelineframework.processor.ir.GrpcBinding safeResolveBinding(
        OrchestratorBinding binding,
        DescriptorProtos.FileDescriptorSet descriptorSet,
        GenerationContext ctx,
        String methodName,
        boolean inputStreaming,
        boolean outputStreaming
    ) {
        try {
            return new OrchestratorGrpcBindingResolver().resolve(
                binding.model(),
                descriptorSet,
                methodName,
                inputStreaming,
                outputStreaming,
                ctx.processingEnv().getMessager());
        } catch (IllegalStateException e) {
            if (ctx.processingEnv() != null && ctx.processingEnv().getMessager() != null) {
                ctx.processingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Skipping orchestrator ingest client generation: " + e.getMessage());
            }
            return null;
        }
    }

    private ClassName resolveSubscribeInputType(
        OrchestratorBinding binding,
        DescriptorProtos.FileDescriptorSet descriptorSet,
        GenerationContext ctx,
        GrpcJavaTypeResolver typeResolver,
        ClassName outputType
    ) {
        var subscribeBinding = safeResolveBinding(binding, descriptorSet, ctx,
            ORCHESTRATOR_SUBSCRIBE_METHOD, false, true);
        if (subscribeBinding == null) {
            return null;
        }

        var subscribeTypes = typeResolver.resolve(subscribeBinding, ctx.processingEnv().getMessager());
        if (subscribeTypes == null) {
            if (ctx.processingEnv() != null && ctx.processingEnv().getMessager() != null) {
                ctx.processingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Skipping orchestrator ingest client generation: could not resolve subscribe types.");
            }
            return null;
        }

        ClassName subscribeInputType = subscribeTypes.grpcParameterType();
        ClassName subscribeOutputType = subscribeTypes.grpcReturnType();
        if (subscribeInputType == null || subscribeOutputType == null) {
            throw new IllegalStateException("Failed to resolve orchestrator subscribe client types from descriptors.");
        }
        if (!subscribeOutputType.equals(outputType) && ctx.processingEnv() != null
            && ctx.processingEnv().getMessager() != null) {
            ctx.processingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.ERROR,
                "Subscribe output type differs from ingest output type; cannot generate a client that assumes "
                    + "the ingest output type for subscribe.");
            return null;
        }
        return subscribeInputType;
    }
}
