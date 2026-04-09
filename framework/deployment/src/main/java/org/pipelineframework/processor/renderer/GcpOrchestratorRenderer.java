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

    /**
     * Specifies the cloud provider identifier used for generated handlers.
     *
     * @return the string "gcp" identifying Google Cloud Platform
     */
    @Override
    protected String getCloudProvider() { return "gcp"; }

    /**
     * The context class used for GCP HTTP function handlers.
     *
     * @return the ClassName representing com.google.cloud.functions.HttpRequest
     */
    @Override
    protected ClassName getContextClassName() { return HTTP_REQUEST; }

    /**
     * Provide the handler interface class used for generated Google Cloud Functions.
     *
     * @return the ClassName corresponding to {@code com.google.cloud.functions.HttpFunction}
     */
    @Override
    protected ClassName getHandlerInterfaceClassName() { return HTTP_FUNCTION; }

    /**
     * Provides the expression used to obtain a request identifier for incoming requests.
     *
     * The generated expression yields the first value of the configured request-id header if present;
     * otherwise it produces a newly generated UUID string.
     *
     * @return a Java expression string that evaluates to the header value when available, or a UUID string when not
     */
    @Override
    protected String getRequestIdExpression() { return "$T.ofNullable(request.getFirstHeader($S).orElse(null)).orElseGet(() -> $T.randomUUID().toString())"; }

    /**
     * Provides the Java expression used to resolve the function name from an environment variable.
     *
     * @return the expression string "System.getenv($S)" where `$S` will be replaced with the environment variable name.
     */
    @Override
    protected String getFunctionNameExpression() { return "System.getenv($S)"; }

    /**
     * Provide the template expression used to extract an execution id from an HttpRequest.
     *
     * <p>The returned format string represents code that reads the first occurrence of the execution-id
     * header (placeholder supplied as a format argument) and, if absent, falls back to a newly
     * generated UUID string.
     *
     * @return a format string that resolves to `request.getFirstHeader(<headerName>).orElseGet(() -> UUID.randomUUID().toString())`
     */
    @Override
    protected String getExecutionIdExpression() { return "request.getFirstHeader($S).orElseGet(() -> $T.randomUUID().toString())"; }

    /**
     * Generates and writes GCP-specific orchestrator HTTP handler and request DTO classes for async execution.
     *
     * This method builds two request DTOs (run-async and execution-lookup) and three HttpFunction handlers
     * (run-async, status, result) configured for Google Cloud Functions, then writes them into
     * {@code basePackage + ".orchestrator.service"} in the provided generation context.
     *
     * @param binding        orchestrator binding describing the pipeline to generate handlers for
     * @param ctx            generation context that provides output directory and utilities
     * @param basePackage    root package under which generated classes will be placed
     * @param inputDto       ClassName of the pipeline input DTO used by generated request/handler code
     * @param outputDto      ClassName of the pipeline output DTO used by result handler and DTOs
     * @param streamingInput true if handlers should accept batched/streaming input
     * @param streamingOutput true if the result handler should return a list (streaming) of outputs
     * @throws IOException if writing generated Java files to the output directory fails
     */
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

    /**
     * Builds the GCP HTTP "service" handler method used to submit an orchestrator run request.
     *
     * The generated method deserializes the request payload into the PipelineRunAsyncRequest envelope,
     * extracts input/inputBatch/tenantId/idempotencyKey from the envelope, invokes
     * `pipelineExecutionService.executePipelineAsync(...)` with the correct execution input and metadata,
     * and writes the accepted execution DTO to the response.
     *
     * @param basePackage the base Java package for generated types (used for naming/placement)
     * @param inputDto the class of the input DTO to deserialize from the request payload
     * @param runAsyncRequestType the request DTO TypeName used for run-async requests
     * @param runAsyncAcceptedDto the class of the DTO returned when a run is accepted
     * @param streamingInput true if the handler should accept streaming input
     * @param streamingOutput true if the pipeline produces streaming output
     * @return a MethodSpec representing the generated public `service(HttpRequest, HttpResponse)` handler method
     */
    private MethodSpec buildGcpRunAsyncHandler(String basePackage, ClassName inputDto, TypeName runAsyncRequestType, ClassName runAsyncAcceptedDto, boolean streamingInput, boolean streamingOutput) {
        ClassName list = ClassName.get(List.class);
        ClassName objectMapper = ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper");
        ClassName duration = ClassName.get("java.time", "Duration");

        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(HTTP_RESPONSE, "response")
            .addException(Exception.class)
            .addStatement("$T mapper = new $T()", objectMapper, objectMapper)
            .addStatement("$T envelope = mapper.readValue(request.getReader(), $T.class)", runAsyncRequestType, runAsyncRequestType)
            .addStatement("$T executionInput", Object.class)
            .beginControlFlow("if (envelope != null && envelope.inputBatch != null && !envelope.inputBatch.isEmpty())")
            .addStatement("executionInput = $T.createFrom().iterable(envelope.inputBatch)", MULTI)
            .nextControlFlow("else if (envelope != null && envelope.input != null)")
            .addStatement("executionInput = $T.createFrom().item(envelope.input)", MULTI)
            .nextControlFlow("else")
            .addStatement("executionInput = $T.createFrom().empty()", MULTI)
            .endControlFlow()
            .addStatement("String tenantId = envelope == null ? null : envelope.tenantId")
            .addStatement("String idempotencyKey = envelope == null ? null : envelope.idempotencyKey")
            .addStatement("$T result = pipelineExecutionService.executePipelineAsync(executionInput, tenantId, idempotencyKey, $L).await().atMost($T.ofSeconds(30))", runAsyncAcceptedDto, streamingOutput, duration)
            .addStatement("response.getWriter().write(mapper.writeValueAsString(result))")
            .addStatement("response.setStatusCode(200)")
            .build();
    }

    /**
     * Handles an HTTP request to retrieve the execution status for a given execution ID.
     *
     * @throws IllegalArgumentException if the "X-Execution-ID" header is missing or blank
     */
    private MethodSpec buildGcpStatusHandler(TypeName executionLookupRequestType, ClassName executionStatusDto) {
        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(HTTP_RESPONSE, "response")
            .addException(Exception.class)
            .addStatement("String executionId = request.getFirstHeader($S).orElse(null)", "X-Execution-ID")
            .addStatement("String tenantId = request.getFirstHeader($S).orElse(null)", "X-Tenant-ID")
            .beginControlFlow("if (executionId == null || executionId.isBlank())")
            .addStatement("throw new IllegalArgumentException($S)", "executionId is required")
            .endControlFlow()
            .addStatement("$T result = pipelineExecutionService.getExecutionStatus(tenantId, executionId).await().atMost($T.ofSeconds(30))", executionStatusDto, ClassName.get("java.time", "Duration"))
            .addStatement("response.getWriter().write(new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(result))")
            .addStatement("response.setStatusCode(200)")
            .build();
    }

    /**
     * Handles an HTTP request to retrieve the result of a previously started execution.
     *
     * Reads the "X-Execution-ID" header (required) and the optional "X-Tenant-ID" header,
     * then returns the execution result (a single DTO or a list of DTOs when streaming).
     *
     * @throws IllegalArgumentException if the "X-Execution-ID" header is missing or blank
     */
    private MethodSpec buildGcpResultHandler(TypeName executionLookupRequestType, TypeName asyncResultType, ClassName outputDto, boolean streamingOutput) {
        return MethodSpec.methodBuilder("service")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID)
            .addParameter(HTTP_REQUEST, "request")
            .addParameter(HTTP_RESPONSE, "response")
            .addException(Exception.class)
            .addStatement("String executionId = request.getFirstHeader($S).orElse(null)", "X-Execution-ID")
            .addStatement("String tenantId = request.getFirstHeader($S).orElse(null)", "X-Tenant-ID")
            .beginControlFlow("if (executionId == null || executionId.isBlank())")
            .addStatement("throw new IllegalArgumentException($S)", "executionId is required")
            .endControlFlow()
            .addStatement("$T result = pipelineExecutionService.<$T>getExecutionResult(tenantId, executionId, $T.class, $L).await().atMost($T.ofSeconds(30))", asyncResultType, outputDto, outputDto, streamingOutput, ClassName.get("java.time", "Duration"))
            .addStatement("response.getWriter().write(new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(result))")
            .addStatement("response.setStatusCode(200)")
            .build();
    }
}
