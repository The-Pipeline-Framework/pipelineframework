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

/**
 * Generates a gRPC ingest client for orchestrator services.
 */
public class OrchestratorIngestClientRenderer {

    private static final String CLIENT_CLASS = "OrchestratorIngestClient";
    private static final String ORCHESTRATOR_INGEST_METHOD = "Ingest";

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
        var grpcBinding = resolver.resolve(
            binding.model(),
            descriptorSet,
            ORCHESTRATOR_INGEST_METHOD,
            true,
            true,
            ctx.processingEnv().getMessager());

        GrpcJavaTypeResolver typeResolver = new GrpcJavaTypeResolver();
        var grpcTypes = typeResolver.resolve(grpcBinding, ctx.processingEnv().getMessager());

        ClassName stubType = grpcTypes.stub();
        ClassName inputType = grpcTypes.grpcParameterType();
        ClassName outputType = grpcTypes.grpcReturnType();
        if (stubType == null || inputType == null || outputType == null) {
            throw new IllegalStateException("Failed to resolve orchestrator ingest client types from descriptors.");
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

        TypeSpec client = TypeSpec.classBuilder(CLIENT_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "Dependent"))
            .addAnnotation(ClassName.get(Unremovable.class))
            .addField(grpcClientField)
            .addMethod(ingestMethod)
            .build();

        JavaFile.builder(binding.basePackage() + ".orchestrator.client", client)
            .build()
            .writeTo(ctx.processingEnv().getFiler());
    }
}
