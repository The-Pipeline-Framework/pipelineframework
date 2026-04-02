/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.processor.ir.OrchestratorBinding;

/**
 * Generates AWS Lambda RequestHandler wrappers for orchestrator execution.
 */
public class AwsLambdaOrchestratorRenderer extends AbstractOrchestratorFunctionHandlerRenderer {

    private static final ClassName LAMBDA_CONTEXT = ClassName.get("com.amazonaws.services.lambda.runtime", "Context");
    private static final ClassName REQUEST_HANDLER = ClassName.get("com.amazonaws.services.lambda.runtime", "RequestHandler");
    private static final ClassName PIPELINE_EXECUTION_SERVICE = ClassName.get("org.pipelineframework", "PipelineExecutionService");
    private static final ClassName RUN_ASYNC_ACCEPTED_DTO = ClassName.get("org.pipelineframework.orchestrator.dto", "RunAsyncAcceptedDto");
    private static final ClassName EXECUTION_STATUS_DTO = ClassName.get("org.pipelineframework.orchestrator.dto", "ExecutionStatusDto");

    /** Returns the fully-qualified class name of the main handler. */
    public static String handlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + HANDLER_CLASS;
    }

    /** Returns the fully-qualified class name of the async handler. */
    public static String runAsyncHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + RUN_ASYNC_HANDLER_CLASS;
    }

    /** Returns the fully-qualified class name of the status handler. */
    public static String statusHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + STATUS_HANDLER_CLASS;
    }

    /** Returns the fully-qualified class name of the result handler. */
    public static String resultHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + RESULT_HANDLER_CLASS;
    }

    @Override
    protected String getCloudProvider() { return "aws"; }

    @Override
    protected ClassName getContextClassName() { return LAMBDA_CONTEXT; }

    @Override
    protected ClassName getHandlerInterfaceClassName() { return REQUEST_HANDLER; }

    @Override
    protected String getRequestIdExpression() { return "context != null ? context.getAwsRequestId() : $S"; }

    @Override
    protected String getFunctionNameExpression() { return "context != null ? context.getFunctionName() : $S"; }

    @Override
    protected String getExecutionIdExpression() { return "context != null ? context.getLogStreamName() : null"; }

    @Override
    protected void renderAsyncHandlers(OrchestratorBinding binding, GenerationContext ctx, String basePackage, ClassName inputDto, ClassName outputDto, boolean streamingInput, boolean streamingOutput) throws IOException {
        ClassName list = ClassName.get(List.class);
        TypeName runAsyncRequestType = ClassName.get(basePackage + ".orchestrator.service", RUN_ASYNC_REQUEST_CLASS);
        TypeName executionLookupRequestType = ClassName.get(basePackage + ".orchestrator.service", EXECUTION_LOOKUP_REQUEST_CLASS);
        TypeName asyncResultType = streamingOutput ? ParameterizedTypeName.get(list, outputDto) : outputDto;

        // Generate request DTOs
        TypeSpec runAsyncRequest = TypeSpec.classBuilder(RUN_ASYNC_REQUEST_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addField(FieldSpec.builder(inputDto, "input", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(ParameterizedTypeName.get(list, inputDto), "inputBatch", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(String.class, "tenantId", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(String.class, "idempotencyKey", Modifier.PUBLIC).build())
            .build();

        TypeSpec executionLookupRequest = TypeSpec.classBuilder(EXECUTION_LOOKUP_REQUEST_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addField(FieldSpec.builder(String.class, "tenantId", Modifier.PUBLIC).build())
            .addField(FieldSpec.builder(String.class, "executionId", Modifier.PUBLIC).build())
            .build();

        // Generate RunAsync handler
        MethodSpec runAsyncHandleRequest = buildRunAsyncHandler(basePackage, inputDto, runAsyncRequestType, RUN_ASYNC_ACCEPTED_DTO, streamingInput, streamingOutput);
        TypeSpec runAsyncHandler = TypeSpec.classBuilder(RUN_ASYNC_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", RUN_ASYNC_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, runAsyncRequestType, RUN_ASYNC_ACCEPTED_DTO))
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(runAsyncHandleRequest)
            .build();

        // Generate Status handler
        MethodSpec statusHandleRequest = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(EXECUTION_STATUS_DTO)
            .addParameter(executionLookupRequestType, "request")
            .addParameter(LAMBDA_CONTEXT, "context")
            .beginControlFlow("if (request == null || request.executionId == null || request.executionId.isBlank())")
            .addStatement("throw new IllegalArgumentException($S)", "executionId is required")
            .endControlFlow()
            .addStatement("return pipelineExecutionService.getExecutionStatus(request.tenantId, request.executionId).await().indefinitely()")
            .build();

        TypeSpec statusHandler = TypeSpec.classBuilder(STATUS_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", STATUS_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, executionLookupRequestType, EXECUTION_STATUS_DTO))
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(statusHandleRequest)
            .build();

        // Generate Result handler
        MethodSpec resultHandleRequest = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(asyncResultType)
            .addParameter(executionLookupRequestType, "request")
            .addParameter(LAMBDA_CONTEXT, "context")
            .beginControlFlow("if (request == null || request.executionId == null || request.executionId.isBlank())")
            .addStatement("throw new IllegalArgumentException($S)", "executionId is required")
            .endControlFlow()
            .addStatement("return pipelineExecutionService.<$T>getExecutionResult(request.tenantId, request.executionId, $T.class, $L).await().indefinitely()", asyncResultType, outputDto, streamingOutput)
            .build();

        TypeSpec resultHandler = TypeSpec.classBuilder(RESULT_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", RESULT_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(ParameterizedTypeName.get(REQUEST_HANDLER, executionLookupRequestType, asyncResultType))
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(resultHandleRequest)
            .build();

        // Write all generated types
        JavaFile.builder(basePackage + ".orchestrator.service", runAsyncRequest).build().writeTo(ctx.outputDir());
        JavaFile.builder(basePackage + ".orchestrator.service", executionLookupRequest).build().writeTo(ctx.outputDir());
        JavaFile.builder(basePackage + ".orchestrator.service", runAsyncHandler).build().writeTo(ctx.outputDir());
        JavaFile.builder(basePackage + ".orchestrator.service", statusHandler).build().writeTo(ctx.outputDir());
        JavaFile.builder(basePackage + ".orchestrator.service", resultHandler).build().writeTo(ctx.outputDir());
    }

    private MethodSpec buildRunAsyncHandler(String basePackage, ClassName inputDto, TypeName runAsyncRequestType, ClassName runAsyncAcceptedDto, boolean streamingInput, boolean streamingOutput) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("handleRequest")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(runAsyncAcceptedDto)
            .addParameter(runAsyncRequestType, "request")
            .addParameter(LAMBDA_CONTEXT, "context")
            .addStatement("$T executionInput", Object.class);

        if (streamingInput) {
            builder.beginControlFlow("if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())")
                .addStatement("executionInput = $T.createFrom().iterable(request.inputBatch)", MULTI)
                .nextControlFlow("else if (request != null && request.input != null)")
                .addStatement("executionInput = $T.createFrom().item(request.input)", MULTI)
                .nextControlFlow("else")
                .addStatement("executionInput = $T.createFrom().empty()", MULTI)
                .endControlFlow();
        } else {
            builder.beginControlFlow("if (request != null && request.inputBatch != null && request.inputBatch.size() > 1)")
                .addStatement("throw new IllegalArgumentException($S)", "RunAsync unary handlers accept at most one item in inputBatch")
                .nextControlFlow("else if (request != null && request.inputBatch != null && !request.inputBatch.isEmpty())")
                .addStatement("executionInput = request.inputBatch.get(0)")
                .nextControlFlow("else if (request != null && request.input != null)")
                .addStatement("executionInput = request.input")
                .nextControlFlow("else")
                .addStatement("executionInput = null")
                .endControlFlow();
        }

        return builder.addStatement("String tenantId = request == null ? null : request.tenantId")
            .addStatement("String idempotencyKey = request == null ? null : request.idempotencyKey")
            .addStatement("return pipelineExecutionService.executePipelineAsync(executionInput, tenantId, idempotencyKey, $L).await().indefinitely()", streamingOutput)
            .build();
    }
}
