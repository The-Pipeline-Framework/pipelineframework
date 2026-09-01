package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.RecordComponentElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import org.pipelineframework.config.template.PipelineTemplateJavaScalarTypes;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.proto.PipelineJavaProtoScalarExpressions;

/** Emits typed REST and protobuf adapters for normalized v3 record bindings. */
final class CanonicalRecordTransportRenderer {
    private final GenerationContext context;
    private final CanonicalTransportBindingResolver resolver;
    private final PipelineTemplateTypeModel typeModel;
    private final String basePackage;
    private final PipelineTemplateJavaScalarTypes scalarTypes = new PipelineTemplateJavaScalarTypes();

    CanonicalRecordTransportRenderer(GenerationContext context, CanonicalTransportBindingResolver resolver) {
        this.context = context;
        this.resolver = resolver;
        this.typeModel = resolver.typeModel().orElseThrow();
        this.basePackage = resolver.basePackage().orElseThrow();
    }

    void ensureRest(CanonicalTransportTypeBinding binding) throws IOException {
        ensureDto(binding.definition());
        ensureRestMapper(binding);
    }

    void ensureGrpc(CanonicalTransportTypeBinding binding) throws IOException {
        ensureGrpcMapper(binding);
    }

    private void ensureDto(PipelineTemplateTypeDefinition.RecordType record) throws IOException {
        for (PipelineTemplateTypeDefinition.Field field : record.fields()) {
            java.util.Optional<PipelineTemplateTypeDefinition.RecordType> nested = namedRecord(field.type());
            if (nested.isPresent()) {
                ensureDto(nested.orElseThrow());
            }
        }
        ClassName dtoType = ClassName.get(basePackage + ".dto", record.name() + "Dto");
        Path output = sourcePath(dtoType);
        if (Files.exists(output)) {
            return;
        }
        String components = record.fields().stream()
            .map(field -> dtoJavaType(field) + " " + field.name())
            .reduce((left, right) -> left + ", " + right)
            .orElse("");
        StringBuilder source = new StringBuilder(header(dtoType.packageName()))
            .append("public record ").append(dtoType.simpleName()).append('(').append(components).append(") {\n")
            .append("    public ").append(dtoType.simpleName()).append(" {\n");
        record.fields().stream().filter(PipelineTemplateTypeDefinition.Field::repeated).forEach(field -> source
            .append("        ").append(field.name()).append(" = java.util.List.copyOf(java.util.Objects.requireNonNull(")
            .append(field.name()).append(", \"").append(field.name()).append(" must not be null\"));\n"));
        source.append("    }\n}\n");
        write(output, source.toString());
    }

    private void ensureRestMapper(CanonicalTransportTypeBinding binding) throws IOException {
        Path output = sourcePath(binding.restMapperType());
        if (Files.exists(output)) {
            return;
        }
        BoundRecord record = bindRecord(binding);
        Map<String, CanonicalTransportTypeBinding> nested = nestedBindings(record);
        for (CanonicalTransportTypeBinding value : nested.values()) {
            ensureRest(value);
        }
        StringBuilder source = new StringBuilder(header(binding.restMapperType().packageName()))
            .append("public final class ").append(binding.restMapperType().simpleName())
            .append(" implements org.pipelineframework.mapper.Mapper<")
            .append(binding.javaType().canonicalName()).append(", ")
            .append(binding.restDtoType().canonicalName()).append("> {\n");
        appendNestedFields(source, nested);
        source.append("    @Override\n    public ").append(binding.javaType().canonicalName())
            .append(" fromExternal(").append(binding.restDtoType().canonicalName()).append(" external) {\n")
            .append("        return new ").append(binding.javaType().canonicalName()).append('(')
            .append(joinExpressions(record, field -> fromRest(field, nested))).append(");\n    }\n\n")
            .append("    @Override\n    public ").append(binding.restDtoType().canonicalName())
            .append(" toExternal(").append(binding.javaType().canonicalName()).append(" domain) {\n")
            .append("        return new ").append(binding.restDtoType().canonicalName()).append('(')
            .append(joinExpressions(record, field -> toRest(field, nested))).append(");\n    }\n}\n");
        write(output, source.toString());
    }

    private void ensureGrpcMapper(CanonicalTransportTypeBinding binding) throws IOException {
        Path output = sourcePath(binding.grpcMapperType());
        if (Files.exists(output)) {
            return;
        }
        BoundRecord record = bindRecord(binding);
        Map<String, CanonicalTransportTypeBinding> nested = nestedBindings(record);
        for (CanonicalTransportTypeBinding value : nested.values()) {
            ensureGrpc(value);
        }
        StringBuilder source = new StringBuilder(header(binding.grpcMapperType().packageName()))
            .append("public final class ").append(binding.grpcMapperType().simpleName()).append(" {\n");
        source.append("    public ").append(binding.grpcMapperType().simpleName()).append("() {\n    }\n\n")
            .append("    public static ").append(binding.javaType().canonicalName()).append(" fromProto(")
            .append(binding.grpcType().canonicalName()).append(" value) {\n")
            .append("        return new ").append(binding.javaType().canonicalName()).append('(')
            .append(joinExpressions(record, field -> fromGrpc(field, nested))).append(");\n    }\n\n")
            .append("    public static ").append(binding.grpcType().canonicalName()).append(" toProto(")
            .append(binding.javaType().canonicalName()).append(" domain) {\n")
            .append("        var builder = ").append(binding.grpcType().canonicalName()).append(".newBuilder();\n");
        for (BoundField field : record.fields()) {
            source.append("        builder.").append(grpcSetter(field)).append('(')
                .append(toGrpc(field, nested)).append(");\n");
        }
        source.append("        return builder.build();\n    }\n\n")
            .append("    public ").append(binding.javaType().canonicalName()).append(" fromGrpc(")
            .append(binding.grpcType().canonicalName()).append(" value) {\n")
            .append("        return fromProto(value);\n    }\n\n")
            .append("    public ").append(binding.grpcType().canonicalName()).append(" toGrpc(")
            .append(binding.javaType().canonicalName()).append(" domain) {\n")
            .append("        return toProto(domain);\n    }\n");
        if (usesPayloadReference(binding.definition())) {
            source.append("\n    private static ").append(basePackage).append(".grpc.PipelineTypes.PayloadReference ")
                .append("toProtoPayloadReference(org.pipelineframework.repository.PayloadReference value) {\n")
                .append("        try {\n")
                .append("            return ").append(basePackage).append(".grpc.PipelineTypes.PayloadReference.parseFrom(\n")
                .append("                org.pipelineframework.proto.PayloadReferenceProtobufCodec.encode(value));\n")
                .append("        } catch (com.google.protobuf.InvalidProtocolBufferException e) {\n")
                .append("            throw new IllegalArgumentException(\"Failed to encode payload_ref\", e);\n")
                .append("        }\n    }\n");
        }
        source.append("}\n");
        write(output, source.toString());
    }

    private BoundRecord bindRecord(CanonicalTransportTypeBinding binding) {
        if (context.processingEnv() == null) {
            throw new IllegalStateException("Normalized transport mapping requires an annotation processing environment");
        }
        TypeElement element = context.processingEnv().getElementUtils().getTypeElement(binding.javaType().canonicalName());
        if (element == null || element.getKind() != ElementKind.RECORD) {
            throw new IllegalStateException("Normalized v3 record '" + binding.canonicalName()
                + "' requires Java record representation " + binding.javaType());
        }
        Map<String, RecordComponentElement> components = new LinkedHashMap<>();
        element.getRecordComponents().forEach(component -> components.put(component.getSimpleName().toString(), component));
        List<BoundField> fields = new ArrayList<>();
        for (PipelineTemplateTypeDefinition.Field field : binding.definition().fields()) {
            RecordComponentElement component = components.get(field.name());
            if (component == null) {
                throw new IllegalStateException("Java record " + binding.javaType() + " is missing canonical field '"
                    + binding.canonicalName() + "." + field.name() + "'");
            }
            fields.add(new BoundField(field, component.asType()));
        }
        return new BoundRecord(binding, List.copyOf(fields));
    }

    private Map<String, CanonicalTransportTypeBinding> nestedBindings(BoundRecord record) {
        Map<String, CanonicalTransportTypeBinding> nested = new LinkedHashMap<>();
        for (BoundField field : record.fields()) {
            if (namedRecord(field.definition().type()).isEmpty()) {
                continue;
            }
            TypeMirror nestedMirror = field.definition().repeated()
                ? listElementType(field.javaType())
                : field.javaType();
            ClassName nestedJavaType = className(nestedMirror);
            String nestedName = field.definition().type().name();
            CanonicalTransportTypeBinding nestedBinding = resolver.resolve(TypeMapping.canonical(nestedJavaType, nestedName))
                .orElseThrow(() -> new IllegalStateException("Cannot resolve nested canonical record '" + nestedName + "'"));
            nested.put(field.definition().name(), nestedBinding);
        }
        return Map.copyOf(nested);
    }

    private void appendNestedFields(StringBuilder source, Map<String, CanonicalTransportTypeBinding> nested) {
        nested.forEach((field, binding) -> source.append("    private final ")
            .append(binding.restMapperType().canonicalName()).append(' ')
            .append(field).append("Mapper = new ")
            .append(binding.restMapperType().canonicalName()).append("();\n"));
        if (!nested.isEmpty()) {
            source.append('\n');
        }
    }

    private String fromRest(BoundField field, Map<String, CanonicalTransportTypeBinding> nested) {
        String value = "external." + field.definition().name() + "()";
        return mapValue(field, value, nested, "fromExternal");
    }

    private String toRest(BoundField field, Map<String, CanonicalTransportTypeBinding> nested) {
        String value = "domain." + field.definition().name() + "()";
        return mapValue(field, value, nested, "toExternal");
    }

    private String fromGrpc(BoundField field, Map<String, CanonicalTransportTypeBinding> nested) {
        String value = "value.get" + capitalized(field.definition().name())
            + (field.definition().repeated() ? "List()" : "()");
        if (field.definition().type() instanceof PipelineTemplateTypeReference.Scalar scalar) {
            String converted = fromProtoScalar(scalar.name(), "item");
            return field.definition().repeated() && !"item".equals(converted)
                ? value + ".stream().map(item -> " + converted + ").toList()"
                : fromProtoScalar(scalar.name(), value);
        }
        return mapGrpcValue(field, value, nested, "fromProto");
    }

    private String toGrpc(BoundField field, Map<String, CanonicalTransportTypeBinding> nested) {
        String value = "domain." + field.definition().name() + "()";
        if (field.definition().type() instanceof PipelineTemplateTypeReference.Scalar scalar) {
            String converted = toProtoScalar(scalar.name(), "item");
            return field.definition().repeated() && !"item".equals(converted)
                ? value + ".stream().map(item -> " + converted + ").toList()"
                : toProtoScalar(scalar.name(), value);
        }
        return mapGrpcValue(field, value, nested, "toProto");
    }

    private String mapValue(
        BoundField field,
        String value,
        Map<String, CanonicalTransportTypeBinding> nested,
        String method
    ) {
        if (!nested.containsKey(field.definition().name())) {
            return field.definition().repeated() ? "java.util.List.copyOf(" + value + ")" : value;
        }
        String mapper = field.definition().name() + "Mapper";
        return field.definition().repeated()
            ? value + ".stream().map(" + mapper + "::" + method + ").toList()"
            : mapper + "." + method + "(" + value + ")";
    }

    private String mapGrpcValue(
        BoundField field,
        String value,
        Map<String, CanonicalTransportTypeBinding> nested,
        String method
    ) {
        CanonicalTransportTypeBinding binding = nested.get(field.definition().name());
        if (binding == null) {
            return field.definition().repeated() ? "java.util.List.copyOf(" + value + ")" : value;
        }
        String mapper = binding.grpcMapperType().canonicalName();
        return field.definition().repeated()
            ? value + ".stream().map(" + mapper + "::" + method + ").toList()"
            : mapper + "." + method + "(" + value + ")";
    }

    private String dtoJavaType(PipelineTemplateTypeDefinition.Field field) {
        String type = field.type() instanceof PipelineTemplateTypeReference.Scalar scalar
            ? scalarTypes.typeName(scalar.name())
            : basePackage + ".dto." + field.type().name() + "Dto";
        return field.repeated() ? "java.util.List<" + type + ">" : type;
    }

    private java.util.Optional<PipelineTemplateTypeDefinition.RecordType> namedRecord(PipelineTemplateTypeReference reference) {
        if (!(reference instanceof PipelineTemplateTypeReference.Named
            || reference instanceof PipelineTemplateTypeReference.Contributed)) {
            return java.util.Optional.empty();
        }
        return typeModel.definition(reference.name())
            .filter(PipelineTemplateTypeDefinition.RecordType.class::isInstance)
            .map(PipelineTemplateTypeDefinition.RecordType.class::cast);
    }

    private TypeMirror listElementType(TypeMirror mirror) {
        if (!(mirror instanceof DeclaredType declared) || declared.getTypeArguments().size() != 1) {
            throw new IllegalStateException("Repeated canonical field requires a parameterized java.util.List: " + mirror);
        }
        return declared.getTypeArguments().getFirst();
    }

    private ClassName className(TypeMirror mirror) {
        TypeName type = TypeName.get(mirror);
        if (!(type instanceof ClassName className)) {
            throw new IllegalStateException("Nested canonical record requires a named Java record type: " + mirror);
        }
        return className;
    }

    private String grpcSetter(BoundField field) {
        return (field.definition().repeated() ? "addAll" : "set") + capitalized(field.definition().name());
    }

    private String fromProtoScalar(String scalar, String expression) {
        return PipelineJavaProtoScalarExpressions.fromProto(scalar, expression,
            value -> "org.pipelineframework.proto.PayloadReferenceProtobufCodec.decode("
                + value + ".toByteArray())");
    }

    private String toProtoScalar(String scalar, String expression) {
        return PipelineJavaProtoScalarExpressions.toProto(
            scalar, expression, value -> "toProtoPayloadReference(" + value + ")");
    }

    private String joinExpressions(BoundRecord record, java.util.function.Function<BoundField, String> renderer) {
        return record.fields().stream().map(renderer).reduce((left, right) -> left + ", " + right).orElse("");
    }

    private boolean usesPayloadReference(PipelineTemplateTypeDefinition.RecordType record) {
        return record.fields().stream().anyMatch(field ->
            field.type() instanceof PipelineTemplateTypeReference.Scalar scalar
                && "payload_ref".equals(scalar.name()));
    }

    private Path sourcePath(ClassName type) {
        return context.outputDir().resolve(type.packageName().replace('.', '/')).resolve(type.simpleName() + ".java");
    }

    private void write(Path output, String source) throws IOException {
        Files.createDirectories(output.getParent());
        Files.writeString(output, source);
    }

    private String header(String packageName) {
        return "package " + packageName + ";\n\n";
    }

    private String capitalized(String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private record BoundRecord(CanonicalTransportTypeBinding binding, List<BoundField> fields) {
    }

    private record BoundField(PipelineTemplateTypeDefinition.Field definition, TypeMirror javaType) {
    }

}
