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
     * Generate the orchestrator gRPC ingest client class and write it to the binding's client package.
     *
     * Resolves protobuf descriptors and gRPC types for the binding, constructs a CDI-managed client
     * containing a GrpcClient field and public `ingest` and `subscribe` methods, and writes the
     * generated Java file to the package derived from the binding's base package.
     *
     * @param binding the orchestrator binding to generate the client for
     * @param ctx the generation context providing descriptor set and processing environment
     * @throws IOException if writing the generated Java file fails
     */
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.descriptorSet();
        if (descriptorSet == null) {
            throw new IllegalStateException("No protobuf descriptor set available for orchestrator ingest client.");
        }

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

    /**
     * Attempts to resolve the gRPC binding for the specified orchestrator method and returns the resolved binding when successful.
     *
     * @param binding the orchestrator binding configuration to resolve against
     * @param descriptorSet the protobuf descriptor set used for resolving service and message types
     * @param ctx the generation context providing processing utilities and a Messager for diagnostics
     * @param methodName the RPC method name to resolve (e.g., ingest or subscribe)
     * @param inputStreaming whether the RPC's input is a stream
     * @param outputStreaming whether the RPC's output is a stream
     * @return the resolved {@code GrpcBinding}, or {@code null} if resolution failed; when resolution fails an informative WARNING diagnostic is emitted if a Messager is available
     */
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

    /**
     * Resolve the gRPC input type for the orchestrator subscribe method and ensure its output matches the provided ingest output type.
     *
     * @param binding the orchestrator binding to inspect
     * @param descriptorSet the protobuf descriptor set used for type resolution
     * @param ctx the generation context (provides processing environment and messager)
     * @param typeResolver the resolver used to map gRPC bindings to Java types
     * @param outputType the expected ingest output type to validate against the subscribe output type
     * @return the subscribe method's gRPC input type, or `null` if the subscribe binding or its types could not be resolved or the output types do not match
     * @throws IllegalStateException if subscribe types are resolved but one or more core types are missing
     */
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