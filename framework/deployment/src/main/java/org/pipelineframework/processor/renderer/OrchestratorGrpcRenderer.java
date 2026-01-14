package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.*;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;
import org.pipelineframework.processor.util.OrchestratorGrpcBindingResolver;

/**
 * Generates gRPC orchestrator service based on pipeline configuration.
 */
public class OrchestratorGrpcRenderer implements PipelineRenderer<OrchestratorBinding> {

    /**
     * Creates a new OrchestratorGrpcRenderer.
     */
    public OrchestratorGrpcRenderer() {
    }

    private static final String GRPC_CLASS = "OrchestratorGrpcService";
    private static final String ORCHESTRATOR_SERVICE = "OrchestratorService";
    private static final String ORCHESTRATOR_METHOD = "Run";

    @Override
    public GenerationTarget target() {
        return GenerationTarget.GRPC_SERVICE;
    }

    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.descriptorSet();
        if (descriptorSet == null) {
            throw new IllegalStateException("No protobuf descriptor set available for orchestrator gRPC generation.");
        }

        ClassName grpcServiceAnnotation = ClassName.get("io.quarkus.grpc", "GrpcService");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName uni = ClassName.get("io.smallrye.mutiny", "Uni");
        ClassName multi = ClassName.get("io.smallrye.mutiny", "Multi");
        ClassName executionService = ClassName.get("org.pipelineframework", "PipelineExecutionService");

        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        var grpcBinding = safeResolveBinding(binding, descriptorSet, ctx);
        if (grpcBinding == null) {
            return;
        }

        GrpcJavaTypeResolver typeResolver = new GrpcJavaTypeResolver();
        var grpcTypes = typeResolver.resolve(grpcBinding, ctx.processingEnv().getMessager());
        ClassName inputType = grpcTypes.grpcParameterType();
        ClassName outputType = grpcTypes.grpcReturnType();
        if (inputType == null || outputType == null) {
            throw new IllegalStateException("Failed to resolve orchestrator gRPC message types from descriptors.");
        }

        ClassName implBase = grpcTypes.implBase();
        if (implBase == null) {
            implBase = ClassName.get(
                binding.basePackage() + ".grpc",
                "Mutiny" + ORCHESTRATOR_SERVICE + "Grpc",
                ORCHESTRATOR_SERVICE + "ImplBase");
        }

        FieldSpec executionField = FieldSpec.builder(executionService, "pipelineExecutionService", Modifier.PRIVATE)
            .addAnnotation(inject)
            .build();

        MethodSpec.Builder runMethod = MethodSpec.methodBuilder("run")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC);

        TypeName returnType = binding.outputStreaming()
            ? ParameterizedTypeName.get(multi, outputType)
            : ParameterizedTypeName.get(uni, outputType);
        runMethod.returns(returnType);

        TypeName inputParamType = binding.inputStreaming()
            ? ParameterizedTypeName.get(multi, inputType)
            : inputType;
        runMethod.addParameter(inputParamType, "input");

        String methodSuffix = binding.outputStreaming() ? "Streaming" : "Unary";
        if (binding.inputStreaming()) {
            runMethod.addStatement("return pipelineExecutionService.executePipeline$L(input)", methodSuffix);
        } else {
            runMethod.addStatement("return pipelineExecutionService.executePipeline$L($T.createFrom().item(input))",
                methodSuffix, uni);
        }

        TypeSpec service = TypeSpec.classBuilder(GRPC_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(grpcServiceAnnotation)
            .superclass(implBase)
            .addField(executionField)
            .addMethod(runMethod.build())
            .build();

        JavaFile.builder(binding.basePackage() + ".orchestrator.service", service)
            .build()
            .writeTo(ctx.processingEnv().getFiler());
    }

    private org.pipelineframework.processor.ir.GrpcBinding safeResolveBinding(
        OrchestratorBinding binding,
        DescriptorProtos.FileDescriptorSet descriptorSet,
        GenerationContext ctx
    ) {
        try {
            return new OrchestratorGrpcBindingResolver().resolve(
                binding.model(),
                descriptorSet,
                ORCHESTRATOR_METHOD,
                binding.inputStreaming(),
                binding.outputStreaming(),
                ctx.processingEnv().getMessager());
        } catch (IllegalStateException e) {
            ctx.processingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Skipping orchestrator gRPC generation: " + e.getMessage());
            return null;
        }
    }
}
