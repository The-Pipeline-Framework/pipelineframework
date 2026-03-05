package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.List;
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
    private static final String ORCHESTRATOR_RUN_ASYNC_METHOD = OrchestratorRpcConstants.RUN_ASYNC_METHOD;
    private static final String ORCHESTRATOR_GET_EXECUTION_STATUS_METHOD =
        OrchestratorRpcConstants.GET_EXECUTION_STATUS_METHOD;
    private static final String ORCHESTRATOR_GET_EXECUTION_RESULT_METHOD =
        OrchestratorRpcConstants.GET_EXECUTION_RESULT_METHOD;
    private static final String ORCHESTRATOR_INGEST_METHOD = OrchestratorRpcConstants.INGEST_METHOD;
    private static final String ORCHESTRATOR_SUBSCRIBE_METHOD = OrchestratorRpcConstants.SUBSCRIBE_METHOD;

    /**
     * Specifies the generation target for this renderer.
     *
     * @return the GenerationTarget for a gRPC service
     */
    @Override
    public GenerationTarget target() {
        return GenerationTarget.GRPC_SERVICE;
    }

    /**
     * Generates and emits a gRPC orchestrator service class based on the provided binding and generation context.
     *
     * The generated service implements the orchestrator RPCs (run, ingest, subscribe), injects pipeline
     * execution and output bus dependencies, and wires telemetry for RPC metrics. Generation is skipped
     * if the binding cannot be resolved against the provided protobuf descriptors.
     *
     * @param binding the orchestrator binding describing RPC names, streaming modes, and package information
     * @param ctx the generation context providing descriptor sets, processing environment, and filer
     * @throws IOException if writing the generated Java file fails
     * @throws IllegalStateException if no protobuf descriptor set is available or required gRPC message types cannot be resolved
     */
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
        ClassName pipelineContextHolder = ClassName.get("org.pipelineframework.context", "PipelineContextHolder");

        var grpcBinding = safeResolveBinding(
            binding, descriptorSet, ctx, ORCHESTRATOR_METHOD, binding.inputStreaming(), binding.outputStreaming());
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
        var runAsyncBinding = safeResolveBinding(
            binding, descriptorSet, ctx, ORCHESTRATOR_RUN_ASYNC_METHOD, false, false);
        var statusBinding = safeResolveBinding(
            binding, descriptorSet, ctx, ORCHESTRATOR_GET_EXECUTION_STATUS_METHOD, false, false);
        var resultBinding = safeResolveBinding(
            binding, descriptorSet, ctx, ORCHESTRATOR_GET_EXECUTION_RESULT_METHOD, false, false);

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
            .addStatement("long startTime = System.nanoTime()")
            .beginControlFlow("if ($T.get() == null)", pipelineContextHolder)
            .addStatement("$1T e = new $1T($2S)",
                ClassName.get(IllegalStateException.class),
                "Missing pipeline context for subscribe request")
            .addStatement("$T.recordGrpcServer($S, $S, $T.fromThrowable(e), System.nanoTime() - startTime)",
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_SUBSCRIBE_METHOD,
                ClassName.get("io.grpc", "Status"))
            .addStatement("return $T.createFrom().failure(e)",
                multi)
            .endControlFlow()
            .addCode("""
                return pipelineOutputBus.stream($T.class)
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime))
                    .onCompletion().invoke(() -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime));
                """,
                outputType,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_SUBSCRIBE_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_SUBSCRIBE_METHOD,
                ClassName.get("io.grpc", "Status"))
            .build();

        MethodSpec runAsyncMethod = buildRunAsyncMethod(
            binding,
            typeResolver,
            ctx,
            runAsyncBinding,
            inputType,
            outputType,
            uni,
            multi);
        MethodSpec executionStatusMethod = buildExecutionStatusMethod(
            typeResolver,
            ctx,
            statusBinding,
            uni);
        MethodSpec executionResultMethod = buildExecutionResultMethod(
            binding,
            typeResolver,
            ctx,
            resultBinding,
            outputType,
            uni);

        TypeSpec.Builder serviceBuilder = TypeSpec.classBuilder(GRPC_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(grpcServiceAnnotation)
            .superclass(implBase)
            .addField(executionField)
            .addField(outputBusField)
            .addMethod(runMethod.build())
            .addMethod(ingestMethod)
            .addMethod(subscribeMethod);
        if (runAsyncMethod != null) {
            serviceBuilder.addMethod(runAsyncMethod);
        }
        if (executionStatusMethod != null) {
            serviceBuilder.addMethod(executionStatusMethod);
        }
        if (executionResultMethod != null) {
            serviceBuilder.addMethod(executionResultMethod);
        }

        TypeSpec service = serviceBuilder.build();

        JavaFile.builder(binding.basePackage() + ".orchestrator.service", service)
            .build()
            .writeTo(ctx.processingEnv().getFiler());
    }

    /**
     * Attempts to resolve a gRPC binding for the specified orchestrator RPC and returns it if available.
     *
     * Resolves a GrpcBinding for the given method name and streaming configuration using protobuf descriptors.
     * If resolution fails with an IllegalStateException, the failure is reported as a warning and `null` is returned.
     *
     * @param binding the orchestrator binding model to resolve against
     * @param descriptorSet the protobuf FileDescriptorSet used for type resolution
     * @param ctx the generation context providing processing environment and utilities
     * @param methodName the RPC method name to resolve
     * @param inputStreaming true if the RPC's input is a stream
     * @param outputStreaming true if the RPC's output is a stream
     * @return the resolved GrpcBinding, or `null` if resolution failed or could not be performed
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
            ctx.processingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Skipping orchestrator gRPC generation: " + e.getMessage());
            return null;
        }
    }

    /**
     * Build the generated `runAsync` gRPC method that starts an asynchronous pipeline execution,
     * adapts request/response gRPC types, and records RPC telemetry.
     *
     * @param binding the orchestrator binding that determines input/output streaming behavior and options
     * @param typeResolver resolver used to obtain the gRPC request and response message types for the runAsync binding
     * @param ctx generation context used for messaging/environment during type resolution
     * @param runAsyncBinding the resolved gRPC binding for the runAsync RPC; when `null` the method returns `null`
     * @param inputType the resolved pipeline input gRPC message type
     * @param outputType the resolved pipeline output gRPC message type
     * @param uni ClassName for the reactive `Uni` container used as the method return wrapper
     * @param multi ClassName for the reactive `Multi` container used for streaming inputs/outputs
     * @return a MethodSpec that implements the `runAsync` RPC, or `null` if `runAsyncBinding` is `null` or the request/response types cannot be resolved
     */
    private MethodSpec buildRunAsyncMethod(
        OrchestratorBinding binding,
        GrpcJavaTypeResolver typeResolver,
        GenerationContext ctx,
        org.pipelineframework.processor.ir.GrpcBinding runAsyncBinding,
        ClassName inputType,
        ClassName outputType,
        ClassName uni,
        ClassName multi
    ) {
        if (runAsyncBinding == null) {
            return null;
        }
        var asyncTypes = typeResolver.resolve(runAsyncBinding, ctx.processingEnv().getMessager());
        ClassName requestType = asyncTypes.grpcParameterType();
        ClassName responseType = asyncTypes.grpcReturnType();
        if (requestType == null || responseType == null) {
            return null;
        }
        MethodSpec.Builder method = MethodSpec.methodBuilder("runAsync")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(uni, responseType))
            .addParameter(requestType, "request")
            .addStatement("long startTime = System.nanoTime()");

        if (binding.inputStreaming()) {
            method.addCode("""
                $T asyncInput = request.getInputBatchCount() > 0
                    ? $T.createFrom().iterable(request.getInputBatchList())
                    : $T.createFrom().item(request.getInput());
                return pipelineExecutionService.executePipelineAsync(
                        asyncInput,
                        request.getTenantId(),
                        request.getIdempotencyKey(),
                        $L)
                    .onItem().transform(accepted -> $T.newBuilder()
                        .setExecutionId(accepted.executionId())
                        .setDuplicate(accepted.duplicate())
                        .setStatusUrl(accepted.statusUrl() == null ? $S : accepted.statusUrl())
                        .setAcceptedAtEpochMs(accepted.submittedAtEpochMs())
                        .build())
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                Object.class,
                multi,
                multi,
                binding.outputStreaming(),
                responseType,
                "",
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_RUN_ASYNC_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_RUN_ASYNC_METHOD,
                ClassName.get("io.grpc", "Status"));
        } else {
            method.addCode("""
                $T asyncInput = request.getInputBatchCount() > 0 ? request.getInputBatch(0) : request.getInput();
                return pipelineExecutionService.executePipelineAsync(
                        asyncInput,
                        request.getTenantId(),
                        request.getIdempotencyKey(),
                        $L)
                    .onItem().transform(accepted -> $T.newBuilder()
                        .setExecutionId(accepted.executionId())
                        .setDuplicate(accepted.duplicate())
                        .setStatusUrl(accepted.statusUrl() == null ? $S : accepted.statusUrl())
                        .setAcceptedAtEpochMs(accepted.submittedAtEpochMs())
                        .build())
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                Object.class,
                binding.outputStreaming(),
                responseType,
                "",
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_RUN_ASYNC_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_RUN_ASYNC_METHOD,
                ClassName.get("io.grpc", "Status"));
        }

        return method.build();
    }

    /**
     * Create the MethodSpec implementation for the gRPC `getExecutionStatus` RPC.
     *
     * <p>The generated method fetches an execution's status from the pipeline execution service,
     * converts the domain status into the gRPC response message, and records RPC metrics for success
     * and failure paths.
     *
     * @param statusBinding the resolved gRPC binding describing the `getExecutionStatus` RPC; used to derive request and response message types
     * @param uni the `Uni` reactive container ClassName used as the method return type
     * @return the `MethodSpec` for `getExecutionStatus`, or `null` if the binding or its request/response types cannot be resolved
     */
    private MethodSpec buildExecutionStatusMethod(
        GrpcJavaTypeResolver typeResolver,
        GenerationContext ctx,
        org.pipelineframework.processor.ir.GrpcBinding statusBinding,
        ClassName uni
    ) {
        if (statusBinding == null) {
            return null;
        }
        var statusTypes = typeResolver.resolve(statusBinding, ctx.processingEnv().getMessager());
        ClassName requestType = statusTypes.grpcParameterType();
        ClassName responseType = statusTypes.grpcReturnType();
        if (requestType == null || responseType == null) {
            return null;
        }

        return MethodSpec.methodBuilder("getExecutionStatus")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(uni, responseType))
            .addParameter(requestType, "request")
            .addStatement("long startTime = System.nanoTime()")
            .addCode("""
                return pipelineExecutionService.getExecutionStatus(request.getTenantId(), request.getExecutionId())
                    .onItem().transform(status -> $T.newBuilder()
                        .setExecutionId(status.executionId())
                        .setStatus(status.status().name())
                        .setCurrentStepIndex(status.stepIndex())
                        .setAttempt(status.attempt())
                        .setVersion(status.version())
                        .setNextDueEpochMs(status.nextDueAtEpochMs())
                        .setUpdatedAtEpochMs(status.updatedAtEpochMs())
                        .setErrorCode(status.errorCode() == null ? $S : status.errorCode())
                        .setErrorMessage(status.errorMessage() == null ? $S : status.errorMessage())
                        .build())
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                responseType,
                "",
                "",
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_GET_EXECUTION_STATUS_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_GET_EXECUTION_STATUS_METHOD,
                ClassName.get("io.grpc", "Status"))
            .build();
    }

    /**
     * Create a MethodSpec for the getExecutionResult gRPC RPC tailored to the binding's streaming configuration.
     *
     * @param binding the orchestrator binding describing pipeline behavior and streaming flags
     * @param typeResolver resolver used to map gRPC bindings to concrete Java message types
     * @param ctx generation context providing processing environment and utilities
     * @param resultBinding the resolved gRPC binding for the getExecutionResult RPC (may be null)
     * @param outputType the Java ClassName for individual pipeline output items
     * @param uni the ClassName representing Uni (reactive single) used as the method's return wrapper
     * @return a MethodSpec that implements the getExecutionResult RPC, or `null` if the binding is null or required request/response types cannot be resolved
     */
    private MethodSpec buildExecutionResultMethod(
        OrchestratorBinding binding,
        GrpcJavaTypeResolver typeResolver,
        GenerationContext ctx,
        org.pipelineframework.processor.ir.GrpcBinding resultBinding,
        ClassName outputType,
        ClassName uni
    ) {
        if (resultBinding == null) {
            return null;
        }
        var resultTypes = typeResolver.resolve(resultBinding, ctx.processingEnv().getMessager());
        ClassName requestType = resultTypes.grpcParameterType();
        ClassName responseType = resultTypes.grpcReturnType();
        if (requestType == null || responseType == null) {
            return null;
        }

        MethodSpec.Builder method = MethodSpec.methodBuilder("getExecutionResult")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(uni, responseType))
            .addParameter(requestType, "request")
            .addStatement("long startTime = System.nanoTime()");

        if (binding.outputStreaming()) {
            TypeName outputListType = ParameterizedTypeName.get(ClassName.get(List.class), outputType);
            method.addCode("""
                return pipelineExecutionService.<$T>getExecutionResult(
                        request.getTenantId(), request.getExecutionId(), $T.class, true)
                    .onItem().transform(items -> $T.newBuilder().addAllItems(items).build())
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                outputListType,
                outputType,
                responseType,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_GET_EXECUTION_RESULT_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_GET_EXECUTION_RESULT_METHOD,
                ClassName.get("io.grpc", "Status"));
        } else {
            method.addCode("""
                return pipelineExecutionService.<$T>getExecutionResult(
                        request.getTenantId(), request.getExecutionId(), $T.class, false)
                    .onItem().transform(result -> {
                        $T.Builder builder = $T.newBuilder();
                        if (result != null) {
                            builder.addItems(result);
                        }
                        return builder.build();
                    })
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                outputType,
                outputType,
                responseType,
                responseType,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_GET_EXECUTION_RESULT_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_GET_EXECUTION_RESULT_METHOD,
                ClassName.get("io.grpc", "Status"));
        }
        return method.build();
    }
}
