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
 * Generates Google Cloud Functions HTTP handlers for orchestrator execution.
 */
public class GcpOrchestratorRenderer extends AbstractOrchestratorFunctionHandlerRenderer {

    private static final ClassName HTTP_FUNCTION = ClassName.get("com.google.cloud.functions", "HttpFunction");
    private static final ClassName HTTP_REQUEST = ClassName.get("com.google.cloud.functions", "HttpRequest");
    private static final ClassName HTTP_RESPONSE = ClassName.get("com.google.cloud.functions", "HttpResponse");
    private static final ClassName PIPELINE_EXECUTION_SERVICE = ClassName.get("org.pipelineframework", "PipelineExecutionService");
    private static final ClassName RUN_ASYNC_ACCEPTED_DTO = ClassName.get("org.pipelineframework.orchestrator.dto", "RunAsyncAcceptedDto");
    private static final ClassName EXECUTION_STATUS_DTO = ClassName.get("org.pipelineframework.orchestrator.dto", "ExecutionStatusDto");

    @Override
    protected String getCloudProvider() { return "gcp"; }

    @Override
    protected ClassName getContextClassName() { return HTTP_REQUEST; }

    @Override
    protected ClassName getHandlerInterfaceClassName() { return HTTP_FUNCTION; }

    @Override
    protected String getRequestIdExpression() { return "$T.ofNullable(request.getFirstHeader($S).orElse(null)).orElseGet(() -> $T.randomUUID().toString())"; }

    @Override
    protected String getFunctionNameExpression() { return "System.getenv($S)"; }

    @Override
    protected String getExecutionIdExpression() { return "request.getFirstHeader($S).orElseGet(() -> $T.randomUUID().toString())"; }

    @Override
    protected void renderAsyncHandlers(OrchestratorBinding binding, GenerationContext ctx, String basePackage, ClassName inputDto, ClassName outputDto, boolean streamingInput, boolean streamingOutput) throws IOException {
        ClassName list = ClassName.get(List.class);
        TypeName runAsyncRequestType = ClassName.get(basePackage + ".orchestrator.service", RUN_ASYNC_REQUEST_CLASS);
        TypeName executionLookupRequestType = ClassName.get(basePackage + ".orchestrator.service", EXECUTION_LOOKUP_REQUEST_CLASS);
        TypeName asyncResultType = streamingOutput ? ParameterizedTypeName.get(list, outputDto) : outputDto;

        // Generate request DTOs (same as AWS/Azure)
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

        // Generate RunAsync handler (GCP-specific)
        MethodSpec runAsyncHandleRequest = buildGcpRunAsyncHandler(basePackage, inputDto, runAsyncRequestType, RUN_ASYNC_ACCEPTED_DTO, streamingInput, streamingOutput);
        TypeSpec runAsyncHandler = TypeSpec.classBuilder(RUN_ASYNC_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", RUN_ASYNC_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(HTTP_FUNCTION)
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(runAsyncHandleRequest)
            .build();

        // Generate Status handler (GCP-specific)
        MethodSpec statusHandleRequest = buildGcpStatusHandler(executionLookupRequestType, EXECUTION_STATUS_DTO);
        TypeSpec statusHandler = TypeSpec.classBuilder(STATUS_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", STATUS_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(HTTP_FUNCTION)
            .addField(FieldSpec.builder(PIPELINE_EXECUTION_SERVICE, "pipelineExecutionService", Modifier.PRIVATE).addAnnotation(INJECT).build())
            .addMethod(statusHandleRequest)
            .build();

        // Generate Result handler (GCP-specific)
        MethodSpec resultHandleRequest = buildGcpResultHandler(executionLookupRequestType, asyncResultType, outputDto, streamingOutput);
        TypeSpec resultHandler = TypeSpec.classBuilder(RESULT_HANDLER_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(APPLICATION_SCOPED)
            .addAnnotation(AnnotationSpec.builder(NAMED).addMember("value", "$S", RESULT_HANDLER_CLASS).build())
            .addAnnotation(AnnotationSpec.builder(GENERATED_ROLE).addMember("value", "$T.$L", ROLE_ENUM, "REST_SERVER").build())
            .addSuperinterface(HTTP_FUNCTION)
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

    private MethodSpec buildGcpRunAsyncHandler(String basePackage, ClassName inputDto, TypeName runAsyncRequestType, ClassName runAsyncAcceptedDto, boolean streamingInput, boolean streamingOutput) {
        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(runAsyncAcceptedDto)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(HTTP_RESPONSE, "response")
            .addException(Exception.class)
            .addStatement("$T executionInput = request.getPayload().deserialize($T.class)", Object.class, inputDto)
            .addStatement("String tenantId = request.getHeaders().getOrDefault($S, null)", "X-Tenant-ID")
            .addStatement("String idempotencyKey = request.getHeaders().getOrDefault($S, null)", "Idempotency-Key")
            .addStatement("return pipelineExecutionService.executePipelineAsync(executionInput, tenantId, idempotencyKey, $L).await().indefinitely()", streamingOutput)
            .build();
    }

    private MethodSpec buildGcpStatusHandler(TypeName executionLookupRequestType, ClassName executionStatusDto) {
        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(executionStatusDto)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(HTTP_RESPONSE, "response")
            .addException(Exception.class)
            .addStatement("String executionId = request.getHeaders().get($S)", "X-Execution-ID")
            .addStatement("String tenantId = request.getHeaders().getOrDefault($S, null)", "X-Tenant-ID")
            .beginControlFlow("if (executionId == null || executionId.isBlank())")
            .addStatement("throw new IllegalArgumentException($S)", "executionId is required")
            .endControlFlow()
            .addStatement("return pipelineExecutionService.getExecutionStatus(tenantId, executionId).await().indefinitely()")
            .build();
    }

    private MethodSpec buildGcpResultHandler(TypeName executionLookupRequestType, TypeName asyncResultType, ClassName outputDto, boolean streamingOutput) {
        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(asyncResultType)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(HTTP_RESPONSE, "response")
            .addException(Exception.class)
            .addStatement("String executionId = request.getHeaders().get($S)", "X-Execution-ID")
            .addStatement("String tenantId = request.getHeaders().getOrDefault($S, null)", "X-Tenant-ID")
            .beginControlFlow("if (executionId == null || executionId.isBlank())")
            .addStatement("throw new IllegalArgumentException($S)", "executionId is required")
            .endControlFlow()
            .addStatement("return pipelineExecutionService.<$T>getExecutionResult(tenantId, executionId, $T.class, $L).await().indefinitely()", asyncResultType, outputDto, streamingOutput)
            .build();
    }
}
