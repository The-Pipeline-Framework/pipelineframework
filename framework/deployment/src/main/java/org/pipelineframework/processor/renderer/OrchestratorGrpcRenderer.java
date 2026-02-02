package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.*;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;
import org.pipelineframework.processor.util.OrchestratorGrpcBindingResolver;
import org.pipelineframework.processor.util.OrchestratorRpcConstants;

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
    private static final String ORCHESTRATOR_METHOD = OrchestratorRpcConstants.RUN_METHOD;
    private static final String ORCHESTRATOR_INGEST_METHOD = OrchestratorRpcConstants.INGEST_METHOD;
    private static final String ORCHESTRATOR_SUBSCRIBE_METHOD = OrchestratorRpcConstants.SUBSCRIBE_METHOD;

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
        ClassName outputBus = ClassName.get("org.pipelineframework", "PipelineOutputBus");

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
        FieldSpec outputBusField = FieldSpec.builder(outputBus, "pipelineOutputBus", Modifier.PRIVATE)
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
        runMethod.addStatement("long startTime = System.nanoTime()");
        if (binding.outputStreaming()) {
            if (binding.inputStreaming()) {
                runMethod.addCode("""
                    return pipelineExecutionService.<$T>executePipeline$L(input)
                        .onItem().invoke(pipelineOutputBus::publish)
                        .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                            System.nanoTime() - startTime))
                        .onCompletion().invoke(() -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime));
                    """,
                    outputType,
                    methodSuffix,
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    ORCHESTRATOR_SERVICE,
                    ORCHESTRATOR_METHOD,
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    ORCHESTRATOR_SERVICE,
                    ORCHESTRATOR_METHOD,
                    ClassName.get("io.grpc", "Status"));
            } else {
                runMethod.addCode("""
                    return pipelineExecutionService.<$T>executePipeline$L($T.createFrom().item(input))
                        .onItem().invoke(pipelineOutputBus::publish)
                        .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                            System.nanoTime() - startTime))
                        .onCompletion().invoke(() -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime));
                    """,
                    outputType,
                    methodSuffix,
                    uni,
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    ORCHESTRATOR_SERVICE,
                    ORCHESTRATOR_METHOD,
                    ClassName.get("io.grpc", "Status"),
                    ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                    ORCHESTRATOR_SERVICE,
                    ORCHESTRATOR_METHOD,
                    ClassName.get("io.grpc", "Status"));
            }
        } else if (binding.inputStreaming()) {
            runMethod.addCode("""
                return pipelineExecutionService.<$T>executePipeline$L(input)
                    .onItem().invoke(pipelineOutputBus::publish)
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                outputType,
                methodSuffix,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_METHOD,
                ClassName.get("io.grpc", "Status"));
        } else {
            runMethod.addCode("""
                return pipelineExecutionService.<$T>executePipeline$L($T.createFrom().item(input))
                    .onItem().invoke(pipelineOutputBus::publish)
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                outputType,
                methodSuffix,
                uni,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_METHOD,
                ClassName.get("io.grpc", "Status"));
        }

        MethodSpec ingestMethod = MethodSpec.methodBuilder("ingest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(multi, outputType))
            .addParameter(ParameterizedTypeName.get(multi, inputType), "input")
            .addStatement("long startTime = System.nanoTime()")
            .addCode("""
                return pipelineExecutionService.<$T>executePipelineStreaming(input)
                    .onItem().invoke(pipelineOutputBus::publish)
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime))
                    .onCompletion().invoke(() -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime));
                """,
                outputType,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_INGEST_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_INGEST_METHOD,
                ClassName.get("io.grpc", "Status"))
            .build();

        MethodSpec subscribeMethod = MethodSpec.methodBuilder("subscribe")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(multi, outputType))
            .addParameter(ClassName.get("com.google.protobuf", "Empty"), "request")
            .addStatement("return pipelineOutputBus.stream($T.class)", outputType)
            .build();

        TypeSpec service = TypeSpec.classBuilder(GRPC_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(grpcServiceAnnotation)
            .superclass(implBase)
            .addField(executionField)
            .addField(outputBusField)
            .addMethod(runMethod.build())
            .addMethod(ingestMethod)
            .addMethod(subscribeMethod)
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
