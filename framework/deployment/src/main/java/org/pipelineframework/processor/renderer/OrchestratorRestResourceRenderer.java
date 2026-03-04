package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.List;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.*;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;

/**
 * Generates REST orchestrator resource based on pipeline configuration.
 */
public class OrchestratorRestResourceRenderer implements PipelineRenderer<OrchestratorBinding> {

    /**
     * Creates a new OrchestratorRestResourceRenderer.
     */
    public OrchestratorRestResourceRenderer() {
    }

    private static final String RESOURCE_CLASS = "PipelineRunResource";

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_RESOURCE;
    }

    /**
     * Generates and writes a REST resource class that exposes orchestrator pipeline endpoints based on the provided binding.
     *
     * <p>The generated class (PipelineRunResource) contains endpoints for running pipelines, ingesting streaming input,
     * and subscribing to pipeline outputs; DTO types, streaming vs unary behaviour, package, and annotations are derived
     * from the given binding.</p>
     *
     * @param binding source of configuration (base package, input/output DTO names, and streaming flags) used to shape the generated resource
     * @param ctx     generation context used to write the produced Java file via the annotation processing Filer
     * @throws IOException if writing the generated Java file to the processing Filer fails
     */
    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        ClassName applicationScoped = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName path = ClassName.get("jakarta.ws.rs", "Path");
        ClassName post = ClassName.get("jakarta.ws.rs", "POST");
        ClassName get = ClassName.get("jakarta.ws.rs", "GET");
        ClassName pathParam = ClassName.get("jakarta.ws.rs", "PathParam");
        ClassName headerParam = ClassName.get("jakarta.ws.rs", "HeaderParam");
        ClassName consumes = ClassName.get("jakarta.ws.rs", "Consumes");
        ClassName produces = ClassName.get("jakarta.ws.rs", "Produces");
        ClassName restStream = ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType");
        ClassName uni = ClassName.get("io.smallrye.mutiny", "Uni");
        ClassName multi = ClassName.get("io.smallrye.mutiny", "Multi");
        ClassName executionService = ClassName.get("org.pipelineframework", "PipelineExecutionService");
        ClassName outputBus = ClassName.get("org.pipelineframework", "PipelineOutputBus");
        ClassName runAsyncAcceptedDto = ClassName.get("org.pipelineframework.orchestrator.dto", "RunAsyncAcceptedDto");
        ClassName executionStatusDto = ClassName.get("org.pipelineframework.orchestrator.dto", "ExecutionStatusDto");

        ClassName inputType = ClassName.get(binding.basePackage() + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputType = ClassName.get(binding.basePackage() + ".common.dto", binding.outputTypeName() + "Dto");

        FieldSpec executionField = FieldSpec.builder(executionService, "pipelineExecutionService", Modifier.PRIVATE)
            .addAnnotation(inject)
            .build();
        FieldSpec outputBusField = FieldSpec.builder(outputBus, "pipelineOutputBus", Modifier.PRIVATE)
            .addAnnotation(inject)
            .build();

        MethodSpec.Builder runMethod = MethodSpec.methodBuilder("run")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(post)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/run").build());

        if (binding.inputStreaming() && binding.outputStreaming()) {
            runMethod.addAnnotation(AnnotationSpec.builder(consumes)
                .addMember("value", "$S", "application/x-ndjson")
                .build());
            runMethod.addAnnotation(AnnotationSpec.builder(produces)
                .addMember("value", "$S", "application/x-ndjson")
                .build());
        }
        if (binding.outputStreaming()) {
            runMethod.addAnnotation(AnnotationSpec.builder(restStream).addMember("value", "$S", "application/json").build());
        }

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
            runMethod.addStatement("return pipelineExecutionService.<$T>executePipeline$L(input)" +
                    ".onItem().invoke(pipelineOutputBus::publish)",
                outputType, methodSuffix);
        } else {
            runMethod.addStatement("return pipelineExecutionService.<$T>executePipeline$L($T.createFrom().item(input))" +
                    ".onItem().invoke(pipelineOutputBus::publish)",
                outputType, methodSuffix, uni);
        }

        MethodSpec ingestMethod = MethodSpec.methodBuilder("ingest")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(post)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/ingest").build())
            .addAnnotation(AnnotationSpec.builder(consumes)
                .addMember("value", "$S", "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(produces)
                .addMember("value", "$S", "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(restStream).addMember("value", "$S", "application/json").build())
            .returns(ParameterizedTypeName.get(multi, outputType))
            .addParameter(ParameterizedTypeName.get(multi, inputType), "input")
            .addStatement("return pipelineExecutionService.<$T>executePipelineStreaming(input)" +
                ".onItem().invoke(pipelineOutputBus::publish)", outputType)
            .build();

        TypeName asyncInputType = binding.inputStreaming()
            ? ParameterizedTypeName.get(ClassName.get(List.class), inputType)
            : inputType;
        MethodSpec.Builder runAsyncMethod = MethodSpec.methodBuilder("runAsync")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(post)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/run-async").build())
            .returns(ParameterizedTypeName.get(uni, runAsyncAcceptedDto))
            .addParameter(asyncInputType, "input")
            .addParameter(ParameterSpec.builder(String.class, "tenantId")
                .addAnnotation(AnnotationSpec.builder(headerParam)
                    .addMember("value", "$S", "x-tenant-id")
                    .build())
                .build())
            .addParameter(ParameterSpec.builder(String.class, "idempotencyKey")
                .addAnnotation(AnnotationSpec.builder(headerParam)
                    .addMember("value", "$S", "Idempotency-Key")
                    .build())
                .build());
        if (binding.inputStreaming()) {
            runAsyncMethod.addStatement(
                "return pipelineExecutionService.executePipelineAsync($T.createFrom().iterable(input), tenantId, idempotencyKey)",
                multi);
        } else {
            runAsyncMethod.addStatement(
                "return pipelineExecutionService.executePipelineAsync(input, tenantId, idempotencyKey)");
        }

        MethodSpec statusMethod = MethodSpec.methodBuilder("status")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(get)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/executions/{executionId}").build())
            .returns(ParameterizedTypeName.get(uni, executionStatusDto))
            .addParameter(ParameterSpec.builder(String.class, "executionId")
                .addAnnotation(AnnotationSpec.builder(pathParam).addMember("value", "$S", "executionId").build())
                .build())
            .addParameter(ParameterSpec.builder(String.class, "tenantId")
                .addAnnotation(AnnotationSpec.builder(headerParam)
                    .addMember("value", "$S", "x-tenant-id")
                    .build())
                .build())
            .addStatement("return pipelineExecutionService.getExecutionStatus(tenantId, executionId)")
            .build();

        TypeName resultReturnType = binding.outputStreaming()
            ? ParameterizedTypeName.get(uni, ParameterizedTypeName.get(ClassName.get(List.class), outputType))
            : ParameterizedTypeName.get(uni, outputType);
        MethodSpec resultMethod = MethodSpec.methodBuilder("result")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(get)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/executions/{executionId}/result").build())
            .returns(resultReturnType)
            .addParameter(ParameterSpec.builder(String.class, "executionId")
                .addAnnotation(AnnotationSpec.builder(pathParam).addMember("value", "$S", "executionId").build())
                .build())
            .addParameter(ParameterSpec.builder(String.class, "tenantId")
                .addAnnotation(AnnotationSpec.builder(headerParam)
                    .addMember("value", "$S", "x-tenant-id")
                    .build())
                .build())
            .addStatement("return pipelineExecutionService.getExecutionResult(tenantId, executionId, $T.class, $L)",
                outputType,
                binding.outputStreaming())
            .build();

        MethodSpec subscribeMethod = MethodSpec.methodBuilder("subscribe")
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(get)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/subscribe").build())
            .addAnnotation(AnnotationSpec.builder(produces)
                .addMember("value", "$S", "application/x-ndjson")
                .build())
            .addAnnotation(AnnotationSpec.builder(restStream).addMember("value", "$S", "application/json").build())
            .returns(ParameterizedTypeName.get(multi, outputType))
            .addStatement("return pipelineOutputBus.stream($T.class)", outputType)
            .build();

        TypeSpec resource = TypeSpec.classBuilder(RESOURCE_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(applicationScoped)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/pipeline").build())
            .addField(executionField)
            .addField(outputBusField)
            .addMethod(runMethod.build())
            .addMethod(runAsyncMethod.build())
            .addMethod(ingestMethod)
            .addMethod(statusMethod)
            .addMethod(resultMethod)
            .addMethod(subscribeMethod)
            .build();

        try {
            JavaFile.builder(binding.basePackage() + ".orchestrator.service", resource)
                .build()
                .writeTo(ctx.processingEnv().getFiler());
        } catch (javax.annotation.processing.FilerException e) {
            // Skip duplicate generation attempts across rounds.
        }
    }
}
