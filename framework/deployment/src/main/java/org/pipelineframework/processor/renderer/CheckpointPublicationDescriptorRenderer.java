package org.pipelineframework.processor.renderer;

import java.io.IOException;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;

/**
 * Generates a checkpoint publication descriptor bean for the current pipeline.
 */
public class CheckpointPublicationDescriptorRenderer {

    public ClassName render(
        String basePackage,
        PipelineCheckpointConfig checkpoint,
        TypeName publicationPayloadType,
        GenerationContext ctx
    ) throws IOException {
        ClassName descriptorType = ClassName.get("org.pipelineframework.checkpoint", "CheckpointPublicationDescriptor");
        ClassName publicationSupportType = ClassName.get("org.pipelineframework.checkpoint", "CheckpointPublicationSupport");
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
            : buildKeyFieldsList(checkpoint.idempotencyKeyFields(), listType);
        MethodSpec keyFieldsMethod = MethodSpec.methodBuilder("idempotencyKeyFields")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ParameterizedTypeName.get(listType, ClassName.get(String.class)))
            .addStatement("return $L", keyFields)
            .build();

        TypeSpec.Builder descriptor = TypeSpec.classBuilder(generatedType)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(descriptorType)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
            .addAnnotation(ClassName.get("io.quarkus.arc", "Unremovable"))
            .addMethod(publicationMethod)
            .addMethod(keyFieldsMethod);

        if (publicationPayloadType != null) {
            descriptor
                .addMethod(MethodSpec.methodBuilder("normalizePayload")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .returns(TypeName.OBJECT)
                    .addParameter(TypeName.OBJECT, "resultPayload")
                    .addStatement("return $T.normalizePayload(resultPayload, $T.class)",
                        publicationSupportType,
                        publicationPayloadType)
                    .build());
        }

        JavaFile.builder(generatedType.packageName(), descriptor.build()).build().writeTo(ctx.processingEnv().getFiler());
        return generatedType;
    }

    private CodeBlock buildKeyFieldsList(java.util.List<String> values, ClassName listType) {
        CodeBlock.Builder builder = CodeBlock.builder().add("$T.of(", listType);
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.add(", ");
            }
            builder.add("$S", values.get(i));
        }
        return builder.add(")").build();
    }
}
