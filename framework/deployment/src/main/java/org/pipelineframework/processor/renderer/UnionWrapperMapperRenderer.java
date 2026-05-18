package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.config.template.PipelineTemplateUnion;
import org.pipelineframework.config.template.PipelineTemplateUnionVariant;

/**
 * Generates framework-owned protobuf wrapper mappers for template unions.
 */
public class UnionWrapperMapperRenderer {

    /**
     * Generates a mapper that composes per-variant mappers and only handles oneof wrapping/unwrapping.
     */
    public ClassName render(
        String basePackage,
        PipelineTemplateUnion union,
        ClassName unionDomainType,
        GenerationContext ctx
    ) throws IOException {
        if (basePackage == null || basePackage.isBlank()) {
            throw new IllegalArgumentException("basePackage is required for union mapper generation");
        }
        if (union == null) {
            throw new IllegalArgumentException("union is required for union mapper generation");
        }
        if (unionDomainType == null) {
            throw new IllegalArgumentException("unionDomainType is required for union mapper generation");
        }

        String mapperPackage = basePackage + ".pipeline.mapper";
        ClassName mapperClassName = ClassName.get(mapperPackage, union.name() + "UnionMapper");
        ClassName protoUnionType = ClassName.get(basePackage + ".grpc", "PipelineTypes", union.name());

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(mapperClassName)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
            .addAnnotation(ClassName.get("io.quarkus.arc", "Unremovable"))
            .addSuperinterface(ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.mapper", "Mapper"),
                unionDomainType,
                protoUnionType));

        List<PipelineTemplateUnionVariant> variants = union.variants().values().stream()
            .sorted(Comparator.comparingInt(PipelineTemplateUnionVariant::number))
            .toList();
        for (PipelineTemplateUnionVariant variant : variants) {
            ClassName domainVariantType = resolveDomainVariantType(unionDomainType, variant, ctx.processingEnv());
            ClassName protoVariantType = ClassName.get(basePackage + ".grpc", "PipelineTypes", variant.type());
            TypeName mapperType = ParameterizedTypeName.get(
                ClassName.get("org.pipelineframework.mapper", "Mapper"),
                domainVariantType,
                protoVariantType);
            typeBuilder.addField(FieldSpec.builder(mapperType, mapperFieldName(variant))
                .addAnnotation(AnnotationSpec.builder(ClassName.get("jakarta.inject", "Inject")).build())
                .build());
        }

        typeBuilder.addMethod(buildFromExternal(union, protoUnionType, unionDomainType, variants, basePackage));
        typeBuilder.addMethod(buildToExternal(union, protoUnionType, unionDomainType, variants, basePackage));

        JavaFile.builder(mapperPackage, typeBuilder.build()).build().writeTo(ctx.outputDir());
        return mapperClassName;
    }

    private MethodSpec buildFromExternal(
        PipelineTemplateUnion union,
        ClassName protoUnionType,
        ClassName unionDomainType,
        List<PipelineTemplateUnionVariant> variants,
        String basePackage
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("fromExternal")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(unionDomainType)
            .addParameter(protoUnionType, "external")
            .addStatement("$T.requireNonNull(external, $S)", ClassName.get("java.util", "Objects"), "external must not be null")
            .addCode("return switch (external.getOutcomeCase()) {\n");

        for (PipelineTemplateUnionVariant variant : variants) {
            builder.addCode("  case $L -> $L.fromExternal(external.get$L());\n",
                toCaseConstant(variant.name()),
                mapperFieldName(variant),
                toGetterSuffix(toProtoFieldName(variant.name())));
        }
        builder.addCode("  case OUTCOME_NOT_SET -> throw new $T($S);\n",
            IllegalArgumentException.class,
            union.name() + " has no selected oneof variant");
        builder.addCode("  default -> throw new $T($S + external.getOutcomeCase());\n",
            IllegalArgumentException.class,
            "Unsupported " + union.name() + " oneof variant: ");
        builder.addCode("};\n");
        return builder.build();
    }

    private MethodSpec buildToExternal(
        PipelineTemplateUnion union,
        ClassName protoUnionType,
        ClassName unionDomainType,
        List<PipelineTemplateUnionVariant> variants,
        String basePackage
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toExternal")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(protoUnionType)
            .addParameter(unionDomainType, "domain")
            .addStatement("$T.requireNonNull(domain, $S)", ClassName.get("java.util", "Objects"), "domain must not be null");

        for (PipelineTemplateUnionVariant variant : variants) {
            ClassName domainVariantType = ClassName.get(unionDomainType.packageName(), variant.type());
            String fieldName = toProtoFieldName(variant.name());
            String valueName = toJavaVariablePrefix(fieldName) + "Value";
            builder.beginControlFlow("if (domain instanceof $T $L)", domainVariantType, valueName)
                .addStatement("return $T.newBuilder().set$L($L.toExternal($L)).build()",
                    protoUnionType,
                    toGetterSuffix(fieldName),
                    mapperFieldName(variant),
                    valueName)
                .endControlFlow();
        }
        builder.addStatement("throw new $T($S + domain.getClass().getName())", IllegalArgumentException.class,
            "Unsupported " + union.name() + " domain variant: ");
        return builder.build();
    }

    private String toJavaVariablePrefix(String protoFieldName) {
        String suffix = toGetterSuffix(protoFieldName);
        if (suffix.isEmpty()) {
            return "variant";
        }
        return Character.toLowerCase(suffix.charAt(0)) + suffix.substring(1);
    }

    private ClassName resolveDomainVariantType(
        ClassName unionDomainType,
        PipelineTemplateUnionVariant variant,
        ProcessingEnvironment processingEnv
    ) {
        ClassName variantType = ClassName.get(unionDomainType.packageName(), variant.type());
        if (processingEnv != null
            && processingEnv.getElementUtils() != null
            && processingEnv.getElementUtils().getTypeElement(variantType.canonicalName()) == null) {
            String message = "Union variant type '" + variantType.canonicalName()
                + "' was not found. Variant message '" + variant.type()
                + "' maps by convention to a Java class in the same package as '"
                + unionDomainType.canonicalName() + "'.";
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message);
            throw new IllegalStateException(message);
        }
        return variantType;
    }

    private String mapperFieldName(PipelineTemplateUnionVariant variant) {
        String name = variant.name();
        if (name == null || name.isBlank()) {
            return "variantMapper";
        }
        return Character.toLowerCase(name.charAt(0)) + name.substring(1) + "Mapper";
    }

    private String toCaseConstant(String variantName) {
        return toProtoFieldName(variantName).toUpperCase(java.util.Locale.ROOT);
    }

    private String toGetterSuffix(String protoFieldName) {
        StringBuilder builder = new StringBuilder();
        for (String part : protoFieldName.split("_")) {
            if (part.isEmpty()) {
                continue;
            }
            builder.append(Character.toUpperCase(part.charAt(0)));
            if (part.length() > 1) {
                builder.append(part.substring(1));
            }
        }
        return builder.toString();
    }

    private String toProtoFieldName(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        char previous = 0;
        for (int i = 0; i < input.length(); i++) {
            char current = input.charAt(i);
            if (!Character.isLetterOrDigit(current)) {
                if (!builder.isEmpty() && builder.charAt(builder.length() - 1) != '_') {
                    builder.append('_');
                }
                previous = current;
                continue;
            }
            if (Character.isUpperCase(current)
                && i > 0
                && previous != 0
                && previous != '_'
                && Character.isLowerCase(previous)) {
                builder.append('_');
            }
            builder.append(Character.toLowerCase(current));
            previous = current;
        }
        String result = builder.toString();
        int start = 0;
        int end = result.length();
        while (start < end && result.charAt(start) == '_') {
            start++;
        }
        while (end > start && result.charAt(end - 1) == '_') {
            end--;
        }
        return result.substring(start, end);
    }
}
