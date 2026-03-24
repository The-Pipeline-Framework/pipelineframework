package org.pipelineframework.processor.renderer;

import java.io.IOException;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;

/**
 * Generates a checkpoint publication descriptor bean for the current pipeline.
 */
public class CheckpointPublicationDescriptorRenderer {

    public ClassName render(String basePackage, PipelineCheckpointConfig checkpoint, GenerationContext ctx) throws IOException {
        ClassName descriptorType = ClassName.get("org.pipelineframework.checkpoint", "CheckpointPublicationDescriptor");
        ClassName listType = ClassName.get("java.util", "List");
        ClassName generatedType = ClassName.get(basePackage + ".orchestrator.service", "PipelineCheckpointPublicationDescriptor");

        MethodSpec publicationMethod = MethodSpec.methodBuilder("publication")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $S", checkpoint.publication())
            .build();

        CodeBlock keyFields = checkpoint.idempotencyKeyFields().isEmpty()
            ? CodeBlock.of("$T.of()", listType)
            : CodeBlock.of("$T.of($L)", listType, joinQuoted(checkpoint.idempotencyKeyFields()));
        MethodSpec keyFieldsMethod = MethodSpec.methodBuilder("idempotencyKeyFields")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(listType, ClassName.get(String.class)))
            .addStatement("return $L", keyFields)
            .build();

        TypeSpec descriptor = TypeSpec.classBuilder(generatedType)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(descriptorType)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
            .addAnnotation(ClassName.get("io.quarkus.arc", "Unremovable"))
            .addMethod(publicationMethod)
            .addMethod(keyFieldsMethod)
            .build();

        JavaFile.builder(generatedType.packageName(), descriptor).build().writeTo(ctx.processingEnv().getFiler());
        return generatedType;
    }

    private String joinQuoted(java.util.List<String> values) {
        return values.stream().map(value -> "\"" + value + "\"").collect(java.util.stream.Collectors.joining(", "));
    }
}
