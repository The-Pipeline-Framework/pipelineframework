package org.pipelineframework.processor.renderer;

import java.io.IOException;
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

    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        ClassName applicationScoped = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName path = ClassName.get("jakarta.ws.rs", "Path");
        ClassName post = ClassName.get("jakarta.ws.rs", "POST");
        ClassName consumes = ClassName.get("jakarta.ws.rs", "Consumes");
        ClassName produces = ClassName.get("jakarta.ws.rs", "Produces");
        ClassName restStream = ClassName.get("org.jboss.resteasy.reactive", "RestStreamElementType");
        ClassName uni = ClassName.get("io.smallrye.mutiny", "Uni");
        ClassName multi = ClassName.get("io.smallrye.mutiny", "Multi");
        ClassName executionService = ClassName.get("org.pipelineframework", "PipelineExecutionService");

        ClassName inputType = ClassName.get(binding.basePackage() + ".common.dto", binding.inputTypeName() + "Dto");
        ClassName outputType = ClassName.get(binding.basePackage() + ".common.dto", binding.outputTypeName() + "Dto");

        FieldSpec executionField = FieldSpec.builder(executionService, "pipelineExecutionService", Modifier.PRIVATE)
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
            runMethod.addStatement("return pipelineExecutionService.executePipeline$L(input)", methodSuffix);
        } else {
            runMethod.addStatement("return pipelineExecutionService.executePipeline$L($T.createFrom().item(input))",
                methodSuffix, uni);
        }

        TypeSpec resource = TypeSpec.classBuilder(RESOURCE_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(applicationScoped)
            .addAnnotation(AnnotationSpec.builder(path).addMember("value", "$S", "/pipeline").build())
            .addField(executionField)
            .addMethod(runMethod.build())
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
