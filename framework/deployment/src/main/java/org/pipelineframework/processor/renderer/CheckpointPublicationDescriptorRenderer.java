package org.pipelineframework.processor.renderer;

import java.io.IOException;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import javax.lang.model.element.Modifier;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;

/**
 * Generates a checkpoint publication descriptor bean for the current pipeline.
 */
public class CheckpointPublicationDescriptorRenderer {

    /**
     * Generates and writes a PipelineCheckpointPublicationDescriptor Java source file for the given checkpoint and returns its type.
     *
     * @param basePackage              base package under which the generated class will be placed (used to form the generated class's package)
     * @param checkpoint               pipeline checkpoint configuration that drives the generated descriptor's content
     * @param publicationPayloadType   the payload type to use when emitting a normalizePayload delegate; must be reifiable (ClassName or ArrayTypeName with reifiable components); may be null if no payload normalization should be generated
     * @param ctx                      generation context providing the annotation processing environment and filer
     * @return                         the ClassName of the generated descriptor class
     * @throws IOException             if writing the generated source file to the processing environment's filer fails
     * @throws IllegalArgumentException if publicationPayloadType is non-null and not reifiable
     */
    public ClassName render(
        String basePackage,
        PipelineCheckpointConfig checkpoint,
        TypeName publicationPayloadType,
        GenerationContext ctx
    ) throws IOException {
        if (publicationPayloadType != null) {
            validateReifiable(publicationPayloadType);
        }
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
                    .returns(Object.class)
                    .addParameter(Object.class, "resultPayload")
                    .addStatement("return $T.normalizePayload(resultPayload, $L)",
                        publicationSupportType,
                        generateClassLiteral(publicationPayloadType))
                    .build());
        }

        JavaFile.builder(generatedType.packageName(), descriptor.build()).build().writeTo(ctx.processingEnv().getFiler());
        return generatedType;
    }

    /**
     * Validates that the given TypeName is reifiable (can be represented at runtime as a Class object).
     * Reifiable types include ClassName and ArrayTypeName with reifiable component types.
     * Non-reifiable types include ParameterizedTypeName, TypeVariableName, and WildcardTypeName.
     *
     * @param type the TypeName to validate
     * @throws IllegalArgumentException if the type is not reifiable
     */
    private void validateReifiable(TypeName type) {
        if (type instanceof ClassName) {
            return;
        }
        if (type instanceof ArrayTypeName arrayType) {
            validateReifiable(arrayType.componentType);
            return;
        }
        if (type instanceof ParameterizedTypeName) {
            throw new IllegalArgumentException(
                "Checkpoint publication payload type must be reifiable; ParameterizedTypeName " + type + " is not reifiable");
        }
        if (type instanceof TypeVariableName) {
            throw new IllegalArgumentException(
                "Checkpoint publication payload type must be reifiable; TypeVariableName " + type + " is not reifiable");
        }
        if (type instanceof WildcardTypeName) {
            throw new IllegalArgumentException(
                "Checkpoint publication payload type must be reifiable; WildcardTypeName " + type + " is not reifiable");
        }
        throw new IllegalArgumentException(
            "Checkpoint publication payload type must be reifiable; type " + type + " is not a recognized reifiable form");
    }

    /**
     * Generates a CodeBlock representing a Class literal for the given TypeName.
     * For ClassName: {@code Foo.class}
     * For ArrayTypeName: {@code Foo[].class}
     *
     * @param type the TypeName to generate a class literal for
     * @return a CodeBlock representing the class literal expression
     */
    private CodeBlock generateClassLiteral(TypeName type) {
        if (type instanceof ClassName className) {
            return CodeBlock.of("$T.class", className);
        }
        if (type instanceof ArrayTypeName arrayType) {
            return CodeBlock.of("$T.class", arrayType);
        }
        throw new IllegalArgumentException("Cannot generate class literal for non-reifiable type: " + type);
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