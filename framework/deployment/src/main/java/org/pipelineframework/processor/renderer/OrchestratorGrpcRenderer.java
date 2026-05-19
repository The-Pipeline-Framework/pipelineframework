package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.List;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;

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
    private static final String ORCHESTRATOR_COMPLETE_AWAIT_METHOD =
        OrchestratorRpcConstants.COMPLETE_AWAIT_METHOD;
    private static final String ORCHESTRATOR_LIST_PENDING_AWAIT_METHOD =
        OrchestratorRpcConstants.LIST_PENDING_AWAIT_METHOD;
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
        var grpcTypes = typeResolver.resolve(grpcBinding, messager(ctx));
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
        var completeAwaitBinding = safeResolveBinding(
            binding, descriptorSet, ctx, ORCHESTRATOR_COMPLETE_AWAIT_METHOD, false, false);
        var listPendingAwaitBinding = safeResolveBinding(
            binding, descriptorSet, ctx, ORCHESTRATOR_LIST_PENDING_AWAIT_METHOD, false, false);

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
        MethodSpec completeAwaitMethod = buildCompleteAwaitMethod(typeResolver, ctx, completeAwaitBinding, uni);
        MethodSpec listPendingAwaitMethod = buildListPendingAwaitMethod(typeResolver, ctx, listPendingAwaitBinding, uni);

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
        if (completeAwaitMethod != null) {
            serviceBuilder.addMethod(completeAwaitMethod);
        }
        if (listPendingAwaitMethod != null) {
            serviceBuilder.addMethod(listPendingAwaitMethod);
        }

        TypeSpec service = serviceBuilder.build();

        JavaFile.builder(binding.basePackage() + ".orchestrator.service", service)
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
                messager(ctx));
        } catch (IllegalStateException e) {
            messager(ctx).printMessage(
                Diagnostic.Kind.WARNING,
                "Skipping orchestrator gRPC generation: " + e.getMessage());
            return null;
        }
    }

    private Messager messager(GenerationContext ctx) {
        ProcessingEnvironment processingEnv = ctx.processingEnv();
        if (processingEnv == null || processingEnv.getMessager() == null) {
            return NoopMessager.INSTANCE;
        }
        return processingEnv.getMessager();
    }

    private enum NoopMessager implements Messager {
        INSTANCE;

        @Override
        public void printMessage(Diagnostic.Kind kind, CharSequence msg) {
            print(kind, msg, null, null, null);
        }

        @Override
        public void printMessage(Diagnostic.Kind kind, CharSequence msg, Element e) {
            print(kind, msg, e, null, null);
        }

        @Override
        public void printMessage(Diagnostic.Kind kind, CharSequence msg, Element e, AnnotationMirror a) {
            print(kind, msg, e, a, null);
        }

        @Override
        public void printMessage(
            Diagnostic.Kind kind,
            CharSequence msg,
            Element e,
            AnnotationMirror a,
            AnnotationValue v) {
            print(kind, msg, e, a, v);
        }

        private void print(Diagnostic.Kind kind, CharSequence msg, Element e, AnnotationMirror a, AnnotationValue v) {
            StringBuilder line = new StringBuilder("OrchestratorGrpcRenderer diagnostic [")
                .append(kind)
                .append("] ")
                .append(msg);
            if (e != null) {
                line.append(" element=").append(e);
            }
            if (a != null) {
                line.append(" annotation=").append(a);
            }
            if (v != null) {
                line.append(" value=").append(v);
            }
            System.err.println(line);
        }
    }

    /**
     * Builds the MethodSpec for the generated `runAsync` gRPC method when a corresponding binding is available.
     *
     * @param binding the orchestrator binding describing pipeline I/O streaming characteristics
     * @param typeResolver resolves gRPC parameter and return types for the provided binding
     * @param ctx the generation context used for messaging and filer access
     * @param runAsyncBinding the resolved gRPC binding for the runAsync RPC (may be null)
     * @param inputType the Java type to use for gRPC input messages
     * @param outputType the Java type to use for gRPC response messages
     * @param uni the Mutiny `Uni` class reference used for unary return types
     * @param multi the Mutiny `Multi` class reference used for streaming types
     * @return a MethodSpec for `runAsync` configured according to the binding and resolved types, or `null` if `runAsyncBinding`
     *         is null or required gRPC types cannot be resolved
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
        var asyncTypes = typeResolver.resolve(runAsyncBinding, messager(ctx));
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
                if (request.getInputBatchCount() > 1) {
                    return $T.createFrom().failure(new $T($S));
                }
                $T asyncInput = request.getInputBatchCount() == 1 ? request.getInputBatch(0) : request.getInput();
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
                uni,
                IllegalArgumentException.class,
                "RunAsync unary pipelines accept at most one item in input_batch.",
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
     * Constructs a MethodSpec for the gRPC `getExecutionStatus` RPC that validates the request,
     * queries the pipeline execution status, maps it to the response proto, and records RPC metrics.
     *
     * <p>The generated method will short-circuit and return a failed `Uni` when `executionId` is
     * null or blank. If `statusBinding` is null or the gRPC request/response types cannot be
     * resolved, this method returns `null` (no method generated).
     *
     * @param typeResolver resolver used to map the binding to gRPC parameter/return types
     * @param ctx the generation context providing environment utilities
     * @param statusBinding the binding describing the `getExecutionStatus` RPC; may be null
     * @param uni the Mutiny `Uni` ClassName used for the method return type
     * @return a MethodSpec for the `getExecutionStatus` RPC, or `null` if the binding or types are unavailable
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
        var statusTypes = typeResolver.resolve(statusBinding, messager(ctx));
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
            .beginControlFlow("if (request.getExecutionId() == null || request.getExecutionId().isBlank())")
            .addStatement("return $T.createFrom().failure(new $T($S))",
                uni,
                IllegalArgumentException.class,
                "executionId is required")
            .endControlFlow()
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
     * Builds the gRPC `getExecutionResult` method implementation for the orchestrator service.
     *
     * Constructs a MethodSpec that implements the gRPC handler which validates the request's
     * executionId, queries pipelineExecutionService for the execution result (streaming or unary
     * depending on the binding), maps the service result to the gRPC response proto, and records
     * RPC metrics for success and failure paths.
     *
     * @param binding the orchestrator binding describing pipeline behavior (used to determine output streaming)
     * @param typeResolver resolver for mapping IR gRPC bindings to concrete gRPC message types
     * @param ctx generation context containing processing utilities and environment
     * @param resultBinding the resolved gRPC binding for the getExecutionResult RPC; if null the method is not generated
     * @param outputType the ClassName of the pipeline output message type
     * @param uni the ClassName representing the Mutiny `Uni` type used for return typing
     * @return a MethodSpec for the `getExecutionResult` RPC handler, or `null` if the resultBinding is null or required gRPC types cannot be resolved
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
        var resultTypes = typeResolver.resolve(resultBinding, messager(ctx));
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
        method.beginControlFlow("if (request.getExecutionId() == null || request.getExecutionId().isBlank())")
            .addStatement("return $T.createFrom().failure(new $T($S))",
                uni,
                IllegalArgumentException.class,
                "executionId is required")
            .endControlFlow();

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

    private MethodSpec buildCompleteAwaitMethod(
        GrpcJavaTypeResolver typeResolver,
        GenerationContext ctx,
        org.pipelineframework.processor.ir.GrpcBinding completeAwaitBinding,
        ClassName uni
    ) {
        if (completeAwaitBinding == null) {
            return null;
        }
        var types = typeResolver.resolve(completeAwaitBinding, messager(ctx));
        ClassName requestType = types.grpcParameterType();
        ClassName responseType = types.grpcReturnType();
        if (requestType == null || responseType == null) {
            return null;
        }
        ClassName command = ClassName.get("org.pipelineframework.awaitable", "AwaitCompletionCommand");
        return MethodSpec.methodBuilder("completeAwait")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(uni, responseType))
            .addParameter(requestType, "request")
            .addStatement("long startTime = System.nanoTime()")
            .addCode("""
                return pipelineExecutionService.completeAwaitInteraction(new $T(
                        request.getTenantId(),
                        request.getInteractionId(),
                        request.getCorrelationId(),
                        request.getIdempotencyKey(),
                        request.getResponseJson(),
                        request.getActor(),
                        $T.currentTimeMillis()))
                    .onItem().transform(result -> $T.newBuilder()
                        .setInteractionId(result.record().interactionId())
                        .setExecutionId(result.record().executionId())
                        .setStepId(result.record().stepId())
                        .setStatus(result.record().status().name())
                        .setDuplicate(result.duplicate())
                        .build())
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                command,
                System.class,
                responseType,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_COMPLETE_AWAIT_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_COMPLETE_AWAIT_METHOD,
                ClassName.get("io.grpc", "Status"))
            .build();
    }

    private MethodSpec buildListPendingAwaitMethod(
        GrpcJavaTypeResolver typeResolver,
        GenerationContext ctx,
        org.pipelineframework.processor.ir.GrpcBinding listPendingAwaitBinding,
        ClassName uni
    ) {
        if (listPendingAwaitBinding == null) {
            return null;
        }
        var types = typeResolver.resolve(listPendingAwaitBinding, messager(ctx));
        ClassName requestType = types.grpcParameterType();
        ClassName responseType = types.grpcReturnType();
        if (requestType == null || responseType == null) {
            return null;
        }
        return MethodSpec.methodBuilder("listPendingAwait")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(uni, responseType))
            .addParameter(requestType, "request")
            .addStatement("long startTime = System.nanoTime()")
            .addCode("""
                return pipelineExecutionService.queryPendingAwaitInteractions(
                        request.getTenantId(),
                        request.getAssignee(),
                        request.getGroup(),
                        request.getStepId(),
                        request.getLimit())
                    .onItem().transform(records -> {
                        $T.Builder builder = $T.newBuilder();
                        for (var record : records) {
                            builder.addInteractionsBuilder()
                                .setInteractionId(record.interactionId())
                                .setCorrelationId(record.correlationId())
                                .setExecutionId(record.executionId())
                                .setStepId(record.stepId())
                                .setStepIndex(record.stepIndex())
                                .setOutputType(record.outputType())
                                .setStatus(record.status().name())
                                .setTransportType(record.transportType())
                                .setDeadlineEpochMs(record.deadlineEpochMs())
                                .setCreatedAtEpochMs(record.createdAtEpochMs())
                                .setUpdatedAtEpochMs(record.updatedAtEpochMs());
                        }
                        return builder.build();
                    })
                    .onItem().invoke(item -> $T.recordGrpcServer($S, $S, $T.OK, System.nanoTime() - startTime))
                    .onFailure().invoke(failure -> $T.recordGrpcServer($S, $S, $T.fromThrowable(failure),
                        System.nanoTime() - startTime));
                """,
                responseType,
                responseType,
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_LIST_PENDING_AWAIT_METHOD,
                ClassName.get("io.grpc", "Status"),
                ClassName.get("org.pipelineframework.telemetry", "RpcMetrics"),
                ORCHESTRATOR_SERVICE,
                ORCHESTRATOR_LIST_PENDING_AWAIT_METHOD,
                ClassName.get("io.grpc", "Status"))
            .build();
    }
}
