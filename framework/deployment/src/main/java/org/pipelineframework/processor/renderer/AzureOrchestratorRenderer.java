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
 * Generates Azure Functions HTTP trigger handlers for orchestrator execution.
 */
public class AzureOrchestratorRenderer extends AbstractOrchestratorFunctionHandlerRenderer {

    private static final ClassName EXECUTION_CONTEXT = ClassName.get("com.microsoft.azure.functions", "ExecutionContext");
    private static final ClassName PIPELINE_EXECUTION_SERVICE = ClassName.get("org.pipelineframework", "PipelineExecutionService");
    private static final ClassName RUN_ASYNC_ACCEPTED_DTO = ClassName.get("org.pipelineframework.orchestrator.dto", "RunAsyncAcceptedDto");
    private static final ClassName EXECUTION_STATUS_DTO = ClassName.get("org.pipelineframework.orchestrator.dto", "ExecutionStatusDto");

    /**
     * Identify the target cloud provider for rendered orchestrator handlers.
     *
     * @return the cloud provider identifier "azure"
     */
    @Override
    protected String getCloudProvider() { return "azure"; }

    /**
     * Provide the ClassName representing the Azure Functions ExecutionContext.
     *
     * @return the ClassName for the Azure Functions `ExecutionContext`
     */
    @Override
    protected ClassName getContextClassName() { return EXECUTION_CONTEXT; }

    /**
     * Handler interface class used for generated Azure orchestrator handlers.
     *
     * @return the ClassName representing the handler interface; for Azure this is java.lang.Object
     */
    @Override
    protected ClassName getHandlerInterfaceClassName() { return ClassName.OBJECT; }

    /**
     * Provides the expression used to extract the request ID from the Azure ExecutionContext.
     *
     * @return the template expression that evaluates to `context.getInvocationId()` when `context` is non-null, otherwise the literal placeholder `$S`
     */
    @Override
    protected String getRequestIdExpression() { return "context != null ? context.getInvocationId() : $S"; }

    /**
     * Provide the runtime expression used to obtain the function name from the Azure ExecutionContext.
     *
     * @return a Java expression string that yields `context.getFunctionName()` when `context` is non-null, or the placeholder `$S` when `context` is null
     */
    @Override
    protected String getFunctionNameExpression() { return "context != null ? context.getFunctionName() : $S"; }

    /**
     * Provide the expression used to obtain the execution ID from the Azure ExecutionContext.
     *
     * @return a Java expression that evaluates to the current execution ID via `context.getInvocationId()`, or `null` when `context` is unavailable
     */
    @Override
    protected String getExecutionIdExpression() { return "context != null ? context.getInvocationId() : null"; }

    /**
     * Generates Azure-specific orchestrator handler classes and request DTOs and writes them to disk.
     *
     * <p>Produces two request DTOs (run async and execution lookup) and three handler classes
     * (RunAsync, Status, Result) in the package {@code basePackage + ".orchestrator.service"}.
     * The generated Status and Result handlers validate that {@code executionId} is present and
     * throw {@link IllegalArgumentException} when it is missing. The RunAsync handler accepts
     * either streaming or unary inputs depending on {@code streamingInput} and returns a result
     * shaped by {@code streamingOutput}.</p>
     *
     * @param binding       orchestrator binding metadata used for generation
     * @param ctx           generation context containing output directory and utilities
     * @param basePackage   base Java package under which generated types are placed
     * @param inputDto      ClassName of the pipeline input DTO
     * @param outputDto     ClassName of the pipeline output DTO
     * @param streamingInput  whether the RunAsync handler should accept streaming input
     * @param streamingOutput whether the RunAsync/Result handlers should produce streaming output
     * @throws IOException if writing the generated Java files to the output directory fails
     */
    @Override
    protected void renderAsyncHandlers(OrchestratorBinding binding, GenerationContext ctx, String basePackage, ClassName inputDto, ClassName outputDto, boolean streamingInput, boolean streamingOutput) throws IOException {
        ClassName list = ClassName.get(List.class);
        TypeName runAsyncRequestType = ClassName.get(basePackage + ".orchestrator.service", RUN_ASYNC_REQUEST_CLASS);
        TypeName executionLookupRequestType = ClassName.get(basePackage + ".orchestrator.service", EXECUTION_LOOKUP_REQUEST_CLASS);
        TypeName asyncResultType = streamingOutput ? ParameterizedTypeName.get(list, outputDto) : outputDto;

        // Generate request DTOs (same as AWS)
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

        // Generate RunAsync handler (Azure-specific)
        MethodSpec runAsyncHandleRequest = buildAzureRunAsyncHandler(basePackage, inputDto, runAsyncRequestType, RUN_ASYNC_ACCEPTED_DTO, streamingInput, streamingOutput);
        TypeSpec runAsyncHandler = TypeSpec.classBuilder(RUN_ASYNC_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", RUN_ASYNC_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(runAsyncHandleRequest)
            .build();

        // Generate Status handler (Azure-specific)
        MethodSpec statusHandleRequest = MethodSpec.methodBuilder("run")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(EXECUTION_STATUS_DTO)
            .addParameter(executionLookupRequestType, "request")
            .addParameter(EXECUTION_CONTEXT, "context")
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
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(statusHandleRequest)
            .build();

        // Generate Result handler (Azure-specific)
        MethodSpec resultHandleRequest = MethodSpec.methodBuilder("run")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(asyncResultType)
            .addParameter(executionLookupRequestType, "request")
            .addParameter(EXECUTION_CONTEXT, "context")
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
     * Builds the MethodSpec for the Azure RunAsync handler's `run` method, which adapts input handling for
     * streaming or unary requests and returns the appropriate accepted DTO.
     *
     * @param basePackage         base package used for generated types
     * @param inputDto            class name of the input DTO
     * @param runAsyncRequestType type of the run-async request parameter
     * @param runAsyncAcceptedDto class name of the accepted/response DTO
     * @param streamingInput      whether the handler accepts streaming input
     * @param streamingOutput     whether the handler produces streaming output
     * @return                    a MethodSpec for the generated `run` method of the Azure RunAsync handler
     */
    private MethodSpec buildAzureRunAsyncHandler(String basePackage, ClassName inputDto, TypeName runAsyncRequestType, ClassName runAsyncAcceptedDto, boolean streamingInput, boolean streamingOutput) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("run")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(runAsyncAcceptedDto)
            .addParameter(runAsyncRequestType, "request")
            .addParameter(EXECUTION_CONTEXT, "context")
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
