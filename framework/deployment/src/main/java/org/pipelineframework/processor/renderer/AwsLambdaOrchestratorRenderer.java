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

    /**
     * Build the fully-qualified class name for the orchestrator main handler.
     *
     * @param basePackage the base Java package to use as the prefix
     * @return the fully-qualified class name of the main handler
     */
    public static String handlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + HANDLER_CLASS;
    }

    /**
     * Get the fully qualified class name of the run-async handler for the given base package.
     *
     * @param basePackage the root package to which the orchestrator service package will be appended
     * @return the fully qualified class name of the run-async handler
     */
    public static String runAsyncHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + RUN_ASYNC_HANDLER_CLASS;
    }

    /**
     * Get the fully qualified class name for the status handler.
     *
     * @param basePackage the root package to prepend (e.g., "com.example")
     * @return the fully qualified class name of the status handler
     */
    public static String statusHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + STATUS_HANDLER_CLASS;
    }

    /**
     * Get the fully-qualified class name for the result handler in the given base package.
     *
     * @param basePackage the root package to which the orchestrator service package is appended
     * @return the fully-qualified class name of the result handler
     */
    public static String resultHandlerFqcn(String basePackage) {
        return basePackage + ".orchestrator.service." + RESULT_HANDLER_CLASS;
    }

    /**
     * Specify the cloud provider identifier used by this renderer.
     *
     * @return the cloud provider identifier "aws"
     */
    @Override
    protected String getCloudProvider() { return "aws"; }

    /**
     * Provides the JavaPoet ClassName for the AWS Lambda Context type used in generated handlers.
     *
     * @return the ClassName corresponding to the AWS Lambda `Context`
     */
    @Override
    protected ClassName getContextClassName() { return LAMBDA_CONTEXT; }

    /**
     * The ClassName representing the AWS Lambda RequestHandler interface used by generated handlers.
     *
     * @return the ClassName for the AWS Lambda `RequestHandler` interface
     */
    @Override
    protected ClassName getHandlerInterfaceClassName() { return REQUEST_HANDLER; }

    /**
     * Provides the expression used in generated code to obtain the AWS Lambda request ID from the Lambda Context or a fallback placeholder.
     *
     * @return a JavaPoet-ready expression that evaluates to `context.getAwsRequestId()` when `context` is non-null, or to a string placeholder otherwise
     */
    @Override
    protected String getRequestIdExpression() { return "context != null ? context.getAwsRequestId() : $S"; }

    /**
     * Supplies the string expression used in generated handlers to obtain the AWS Lambda function name from the Lambda Context.
     *
     * @return the expression string that evaluates to the function name or a placeholder when the Context is null
     */
    @Override
    protected String getFunctionNameExpression() { return "context != null ? context.getFunctionName() : $S"; }

    /**
     * Provides the Java expression used to obtain an execution identifier from the AWS Lambda context.
     *
     * @return a String containing a Java expression that yields the Lambda log stream name when `context` is non-null, or `null` otherwise
     */
    @Override
    protected String getExecutionIdExpression() { return "context != null ? context.getLogStreamName() : null"; }

    /**
     * Generates AWS Lambda request DTOs and RequestHandler implementations for async orchestrator operations and writes them into
     * the `${basePackage}.orchestrator.service` package in the generator output directory.
     *
     * <p>Produced types:
     * <ul>
     *   <li>RunAsync request DTO (fields: `input`, `inputBatch`, `tenantId`, `idempotencyKey`)</li>
     *   <li>ExecutionLookup request DTO (fields: `tenantId`, `executionId`)</li>
     *   <li>RunAsync Lambda handler that accepts the RunAsync request and submits an execution</li>
     *   <li>Status Lambda handler that validates `executionId` and returns execution status</li>
     *   <li>Result Lambda handler that validates `executionId` and returns the execution result (single or list depending on streamingOutput)</li>
     * </ul>
     *
     * @param binding the orchestrator binding definition used to drive generation
     * @param ctx the generation context including output directory
     * @param basePackage the base Java package under which generated types will be placed
     * @param inputDto the ClassName representing the input DTO type
     * @param outputDto the ClassName representing the output DTO type
     * @param streamingInput true if the orchestrator accepts streaming input (affects RunAsync handler input shape)
     * @param streamingOutput true if the orchestrator produces streaming output (affects Result handler return type)
     * @throws IOException if writing the generated Java files to the output directory fails
     */
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

    /**
     * Builds the MethodSpec for the generated `handleRequest` method of the run-async AWS Lambda handler.
     *
     * The generated method selects the execution input from the request (using `inputBatch` or `input`),
     * enforces unary constraints when `streamingInput` is false, extracts `tenantId` and `idempotencyKey`
     * from the request, and invokes `pipelineExecutionService.executePipelineAsync(...)` returning its result.
     *
     * @param basePackage         base package used when generating type references
     * @param inputDto            the DTO ClassName for individual input elements
     * @param runAsyncRequestType the request type for the handler
     * @param runAsyncAcceptedDto the accepted response type returned by the handler
     * @param streamingInput      true when input should be treated as a stream (accepts inputBatch); false for unary input
     * @param streamingOutput     true when the pipeline returns streaming output (List of outputs); false for single output
     * @return a MethodSpec that implements the `handleRequest` method for the run-async handler
     */
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
