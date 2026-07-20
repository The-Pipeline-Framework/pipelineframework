/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.proto;

import java.nio.file.Path;
import java.util.*;
import javax.lang.model.SourceVersion;

import org.pipelineframework.config.template.*;

/**
 * Renders nominal Java v3 domain records and their protobuf conversion helpers.
 *
 * <p>Record component order intentionally follows the authored YAML order. Null component values
 * preserve transport presence and do not model domain-validity constraints.</p>
 */
final class PipelineJavaDomainRenderer {
    private static final String ADAPTER_NAME = "PipelineDomainProtoAdapters";
    private static final String VALIDATION_NAME = "PipelineDomainValidation";
    private static final String PAYLOAD_REFERENCE = "org.pipelineframework.repository.PayloadReference";

    List<RenderedSource> render(PipelineV3GenerationPlan plan) {
        validate(plan);
        String domainPackage = plan.basePackage() + ".domain";
        List<String> names = new ArrayList<>(plan.typeModel().definitions().keySet());
        Collections.sort(names);
        List<RenderedSource> sources = new ArrayList<>();
        for (String name : names) {
            PipelineTemplateTypeDefinition definition = plan.typeModel().definitions().get(name);
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                sources.add(source(domainPackage, record.name(), renderRecord(domainPackage, record, plan)));
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                sources.add(source(domainPackage, wrapper.name(), renderWrapper(domainPackage, wrapper, plan)));
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                sources.add(source(domainPackage, union.name(), renderUnion(domainPackage, union, plan)));
            }
        }
        if (hasConstrainedWrappers(plan)) {
            sources.add(source(domainPackage, VALIDATION_NAME, renderValidationHelper(domainPackage)));
        }
        sources.add(source(domainPackage, ADAPTER_NAME, renderAdapters(domainPackage, plan)));
        return List.copyOf(sources);
    }

    private RenderedSource source(String domainPackage, String simpleName, String content) {
        return new RenderedSource(Path.of(domainPackage.replace('.', '/')).resolve(simpleName + ".java"), content);
    }

    private void validate(PipelineV3GenerationPlan plan) {
        if (plan.typeModel().definitions().containsKey(ADAPTER_NAME)) {
            throw new IllegalStateException("Version 3 type '" + ADAPTER_NAME + "' conflicts with generated Java adapter code.");
        }
        if (hasConstrainedWrappers(plan) && plan.typeModel().definitions().containsKey(VALIDATION_NAME)) {
            throw new IllegalStateException("Version 3 type '" + VALIDATION_NAME + "' conflicts with generated Java validation code.");
        }
        for (PipelineTemplateTypeDefinition definition : plan.typeModel().definitions().values()) {
            validateTypeIdentifier("type", definition.name());
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                for (PipelineTemplateTypeDefinition.Field field : record.fields()) {
                    validateIdentifier("field '" + record.name() + "." + field.name() + "'", field.name());
                    resolveJavaType(field.type(), plan.typeModel());
                }
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                resolveJavaType(wrapper.wraps(), plan.typeModel());
            } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
                resolveJavaType(alias.target(), plan.typeModel());
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                validateUnion(union, plan.typeModel());
            }
        }
    }

    private void validateUnion(PipelineTemplateTypeDefinition.UnionType union, PipelineTemplateTypeModel model) {
        Set<String> javaVariantTypes = new HashSet<>();
        for (PipelineTemplateTypeDefinition.Variant variant : union.variants().values()) {
            String javaType = javaVariantTypeName(variant.discriminator());
            validateTypeIdentifier("variant '" + union.name() + "." + variant.discriminator() + "'", javaType);
            if (!javaVariantTypes.add(javaType)) {
                throw new IllegalStateException("Version 3 Java domain target cannot represent union '" + union.name()
                    + "': discriminators normalize to the same Java variant type '" + javaType + "'.");
            }
            resolveJavaType(variant.payload(), model);
        }
    }

    private void validateIdentifier(String subject, String value) {
        if (!SourceVersion.isIdentifier(value) || SourceVersion.isKeyword(value)) {
            throw new IllegalStateException("Version 3 Java domain target cannot represent " + subject + " '" + value
                + "'; use a valid non-keyword Java identifier.");
        }
    }

    private void validateTypeIdentifier(String subject, String value) {
        if (Set.of("record", "sealed", "permits", "non-sealed", "var", "yield").contains(value)) {
            throw new IllegalStateException("Version 3 Java domain target cannot represent " + subject + " '" + value
                + "'; use a valid non-keyword Java identifier.");
        }
        validateIdentifier(subject, value);
    }

    private String renderRecord(
        String domainPackage,
        PipelineTemplateTypeDefinition.RecordType record,
        PipelineV3GenerationPlan plan
    ) {
        StringBuilder builder = header(domainPackage);
        builder.append("/** Generated from the version 3 pipeline type '").append(record.name()).append("'. */\n");
        builder.append("public record ").append(record.name()).append("(\n");
        for (int i = 0; i < record.fields().size(); i++) {
            PipelineTemplateTypeDefinition.Field field = record.fields().get(i);
            builder.append("    ").append(resolveJavaType(field.type(), plan.typeModel())).append(' ').append(field.name());
            builder.append(i + 1 == record.fields().size() ? "\n" : ",\n");
        }
        return builder.append(") {\n}\n").toString();
    }

    private String renderWrapper(
        String domainPackage,
        PipelineTemplateTypeDefinition.WrapperType wrapper,
        PipelineV3GenerationPlan plan
    ) {
        StringBuilder builder = header(domainPackage);
        builder.append("/** Nominal wrapper generated from the version 3 pipeline type '").append(wrapper.name()).append("'. */\n");
        builder.append("public record ").append(wrapper.name()).append("(")
            .append(resolveJavaType(wrapper.wraps(), plan.typeModel())).append(" value) {\n");
        wrapper.constraints().pattern().ifPresent(pattern -> builder
            .append("    private static final java.util.regex.Pattern PATTERN = java.util.regex.Pattern.compile(\"")
            .append(javaStringLiteral(pattern)).append("\");\n"));
        builder.append("    public ").append(wrapper.name()).append(" {\n")
            .append("        if (value == null) { throw new IllegalArgumentException(\"")
            .append(javaStringLiteral(wrapper.name())).append(" value must not be null.\"); }\n");
        if (!wrapper.constraints().isEmpty()) {
            renderWrapperConstraints(builder, wrapper);
        }
        return builder.append("    }\n}\n").toString();
    }

    private String renderUnion(
        String domainPackage,
        PipelineTemplateTypeDefinition.UnionType union,
        PipelineV3GenerationPlan plan
    ) {
        StringBuilder builder = header(domainPackage);
        List<PipelineTemplateTypeDefinition.Variant> variants = union.variants().values().stream()
            .sorted(Comparator.comparing(PipelineTemplateTypeDefinition.Variant::discriminator))
            .toList();
        builder.append("/** Generated sealed domain union for version 3 type '").append(union.name()).append("'. */\n")
            .append("public sealed interface ").append(union.name()).append(" permits ");
        for (int i = 0; i < variants.size(); i++) {
            builder.append(union.name()).append('.').append(javaVariantTypeName(variants.get(i).discriminator()));
            builder.append(i + 1 == variants.size() ? " {\n" : ", ");
        }
        builder.append("    /** The stable discriminator authored in the pipeline type declaration. */\n")
            .append("    String discriminator();\n\n");
        for (PipelineTemplateTypeDefinition.Variant variant : variants) {
            String javaType = javaVariantTypeName(variant.discriminator());
            builder.append("    record ").append(javaType).append('(')
                .append(resolveJavaType(variant.payload(), plan.typeModel())).append(" value) implements ")
                .append(union.name()).append(" {\n")
                .append("        @Override public String discriminator() { return \"")
                .append(javaStringLiteral(variant.discriminator())).append("\"; }\n")
                .append("    }\n\n");
        }
        return builder.append("}\n").toString();
    }

    private String renderAdapters(String domainPackage, PipelineV3GenerationPlan plan) {
        String protoTypes = plan.basePackage() + ".grpc.PipelineTypes";
        StringBuilder builder = header(domainPackage);
        builder.append("/**\n")
            .append(" * Generated protobuf adapters for version 3 domain types.\n")
            .append(" * This surface is intentionally provisional while union APIs are introduced.\n")
            .append(" */\n")
            .append("public final class ").append(ADAPTER_NAME).append(" {\n")
            .append("    private ").append(ADAPTER_NAME).append("() {\n    }\n\n");
        List<String> names = new ArrayList<>(plan.typeModel().definitions().keySet());
        Collections.sort(names);
        for (String name : names) {
            PipelineTemplateTypeDefinition definition = plan.typeModel().definitions().get(name);
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                renderRecordAdapters(builder, record, plan, protoTypes);
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                renderWrapperAdapters(builder, wrapper, plan, protoTypes);
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                renderUnionAdapters(builder, union, plan, protoTypes);
            }
        }
        if (usesPayloadReference(plan)) {
            renderPayloadReferenceAdapters(builder, protoTypes);
        }
        return builder.append("}\n").toString();
    }

    private void renderRecordAdapters(
        StringBuilder builder,
        PipelineTemplateTypeDefinition.RecordType record,
        PipelineV3GenerationPlan plan,
        String protoTypes
    ) {
        Map<String, PipelineIdlSnapshot.TypeFieldSnapshot> state = fieldsByName(record.name(), plan.idlState());
        builder.append("    public static ").append(protoTypes).append('.').append(record.name()).append(" toProto(")
            .append(record.name()).append(" value) {\n")
            .append("        if (value == null) { return null; }\n")
            .append("        var builder = ").append(protoTypes).append('.').append(record.name()).append(".newBuilder();\n");
        for (PipelineTemplateTypeDefinition.Field field : record.fields()) {
            PipelineIdlSnapshot.TypeFieldSnapshot fieldState = requiredFieldState(record.name(), field.name(), state);
            String accessor = "value." + field.name() + "()";
            builder.append("        if (").append(accessor).append(" != null) { builder.")
                .append("set").append(javaSetter(fieldState.protoName())).append('(')
                .append(toProtoExpression(field.type(), accessor, plan.typeModel())).append("); }\n");
        }
        builder.append("        return builder.build();\n    }\n\n");

        builder.append("    public static ").append(record.name()).append(" fromProto(").append(protoTypes).append('.')
            .append(record.name()).append(" value) {\n")
            .append("        if (value == null) { return null; }\n")
            .append("        return new ").append(record.name()).append("(\n");
        for (int i = 0; i < record.fields().size(); i++) {
            PipelineTemplateTypeDefinition.Field field = record.fields().get(i);
            PipelineIdlSnapshot.TypeFieldSnapshot fieldState = requiredFieldState(record.name(), field.name(), state);
            String getter = "value.get" + javaSetter(fieldState.protoName()) + "()";
            builder.append("            value.has").append(javaSetter(fieldState.protoName())).append("() ? ")
                .append(fromProtoExpression(field.type(), getter, plan.typeModel())).append(" : null")
                .append(i + 1 == record.fields().size() ? "\n" : ",\n");
        }
        builder.append("        );\n    }\n\n");
    }

    private void renderWrapperAdapters(
        StringBuilder builder,
        PipelineTemplateTypeDefinition.WrapperType wrapper,
        PipelineV3GenerationPlan plan,
        String protoTypes
    ) {
        builder.append("    public static ").append(protoTypes).append('.').append(wrapper.name()).append(" toProto(")
            .append(wrapper.name()).append(" value) {\n")
            .append("        if (value == null) { return null; }\n")
            .append("        var builder = ").append(protoTypes).append('.').append(wrapper.name()).append(".newBuilder();\n")
            .append("        builder.setValue(")
            .append(toProtoExpression(wrapper.wraps(), "value.value()", plan.typeModel())).append(");\n")
            .append("        return builder.build();\n    }\n\n")
            .append("    public static ").append(wrapper.name()).append(" fromProto(").append(protoTypes).append('.')
            .append(wrapper.name()).append(" value) {\n")
            .append("        if (value == null) { return null; }\n")
            .append("        return new ").append(wrapper.name()).append("(value.hasValue() ? ")
            .append(fromProtoExpression(wrapper.wraps(), "value.getValue()", plan.typeModel())).append(" : null);\n    }\n\n");
    }

    private boolean hasConstrainedWrappers(PipelineV3GenerationPlan plan) {
        return plan.typeModel().definitions().values().stream()
            .anyMatch(definition -> definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper
                && !wrapper.constraints().isEmpty());
    }

    private void renderWrapperConstraints(StringBuilder builder, PipelineTemplateTypeDefinition.WrapperType wrapper) {
        PipelineTemplateWrapperConstraints constraints = wrapper.constraints();
        String scalar = wrapper.wraps().name();
        if ("string".equals(scalar)) {
            builder.append("        ").append(VALIDATION_NAME).append(".validateString(\"")
                .append(javaStringLiteral(wrapper.name())).append("\", value, ")
                .append(optionalIntExpression(constraints.minLength())).append(", ")
                .append(optionalIntExpression(constraints.maxLength())).append(", ")
                .append(patternExpression(constraints)).append(", ")
                .append(constraints.format().isPresent()).append(");\n");
            return;
        }
        String bounds = numericBoundsExpression(constraints);
        String method = switch (scalar) {
            case "int32" -> "validateInt32";
            case "int64" -> "validateInt64";
            case "float32" -> "validateFloat32";
            case "float64" -> "validateFloat64";
            case "decimal" -> "validateDecimal";
            default -> throw new IllegalStateException("Unsupported constrained wrapper scalar '" + scalar + "'.");
        };
        builder.append("        ").append(VALIDATION_NAME).append('.').append(method).append("(\"")
            .append(javaStringLiteral(wrapper.name())).append("\", value, ").append(bounds).append(");\n");
    }

    private String optionalIntExpression(Optional<Integer> value) {
        return value.map(integer -> "java.util.OptionalInt.of(" + integer + ")").orElse("java.util.OptionalInt.empty()");
    }

    private String patternExpression(PipelineTemplateWrapperConstraints constraints) {
        return constraints.pattern().isPresent()
            ? "java.util.Optional.of(PATTERN)"
            : "java.util.Optional.empty()";
    }

    private String numericBoundsExpression(PipelineTemplateWrapperConstraints constraints) {
        return "new " + VALIDATION_NAME + ".NumericBounds(" + optionalDecimalExpression(constraints.minimum()) + ", "
            + optionalDecimalExpression(constraints.minimumExclusive()) + ", " + optionalDecimalExpression(constraints.maximum()) + ", "
            + optionalDecimalExpression(constraints.maximumExclusive()) + ")";
    }

    private String optionalDecimalExpression(Optional<java.math.BigDecimal> value) {
        return value.map(decimal -> "java.util.Optional.of(new java.math.BigDecimal(\"" + decimal.toPlainString() + "\"))")
            .orElse("java.util.Optional.empty()");
    }

    private String renderValidationHelper(String domainPackage) {
        StringBuilder builder = header(domainPackage);
        builder.append("/** Internal Java realization of v3 wrapper constraints. */\n")
            .append("final class ").append(VALIDATION_NAME).append(" {\n")
            .append("    private ").append(VALIDATION_NAME).append("() { }\n\n")
            .append("    static void validateString(String wrapper, String value, java.util.OptionalInt minLength, java.util.OptionalInt maxLength, java.util.Optional<java.util.regex.Pattern> pattern, boolean email) {\n")
            .append("        int length = value.codePointCount(0, value.length());\n")
            .append("        if (minLength.isPresent() && length < minLength.getAsInt()) { throw new IllegalArgumentException(wrapper + \" must contain at least \" + minLength.getAsInt() + \" Unicode code points.\"); }\n")
            .append("        if (maxLength.isPresent() && length > maxLength.getAsInt()) { throw new IllegalArgumentException(wrapper + \" must contain at most \" + maxLength.getAsInt() + \" Unicode code points.\"); }\n")
            .append("        if (pattern.isPresent() && !pattern.get().matcher(value).matches()) { throw new IllegalArgumentException(wrapper + \" does not match its declared pattern.\"); }\n")
            .append("        if (email && !isPracticalEmail(value)) { throw new IllegalArgumentException(wrapper + \" must be a practical mailbox address.\"); }\n")
            .append("    }\n\n")
            .append("    static void validateInt32(String wrapper, Integer value, NumericBounds bounds) { validateNumeric(wrapper, java.math.BigDecimal.valueOf(value.longValue()), bounds); }\n")
            .append("    static void validateInt64(String wrapper, Long value, NumericBounds bounds) { validateNumeric(wrapper, java.math.BigDecimal.valueOf(value), bounds); }\n")
            .append("    static void validateFloat32(String wrapper, Float value, NumericBounds bounds) {\n")
            .append("        if (!Float.isFinite(value)) { throw new IllegalArgumentException(wrapper + \" must be finite.\"); }\n")
            .append("        validateNumeric(wrapper, new java.math.BigDecimal(Float.toString(value)), bounds);\n    }\n")
            .append("    static void validateFloat64(String wrapper, Double value, NumericBounds bounds) {\n")
            .append("        if (!Double.isFinite(value)) { throw new IllegalArgumentException(wrapper + \" must be finite.\"); }\n")
            .append("        validateNumeric(wrapper, new java.math.BigDecimal(Double.toString(value)), bounds);\n    }\n")
            .append("    static void validateDecimal(String wrapper, java.math.BigDecimal value, NumericBounds bounds) { validateNumeric(wrapper, value, bounds); }\n\n")
            .append("    private static void validateNumeric(String wrapper, java.math.BigDecimal value, NumericBounds bounds) {\n")
            .append("        if (bounds.minimum().isPresent() && value.compareTo(bounds.minimum().get()) < 0) { throw new IllegalArgumentException(wrapper + \" is below its minimum.\"); }\n")
            .append("        if (bounds.minimumExclusive().isPresent() && value.compareTo(bounds.minimumExclusive().get()) <= 0) { throw new IllegalArgumentException(wrapper + \" must exceed its exclusive minimum.\"); }\n")
            .append("        if (bounds.maximum().isPresent() && value.compareTo(bounds.maximum().get()) > 0) { throw new IllegalArgumentException(wrapper + \" exceeds its maximum.\"); }\n")
            .append("        if (bounds.maximumExclusive().isPresent() && value.compareTo(bounds.maximumExclusive().get()) >= 0) { throw new IllegalArgumentException(wrapper + \" must be below its exclusive maximum.\"); }\n")
            .append("    }\n\n")
            .append("    private static boolean isPracticalEmail(String value) {\n")
            .append("        if (value.chars().anyMatch(Character::isWhitespace)) { return false; }\n")
            .append("        int at = value.indexOf('@');\n")
            .append("        if (at <= 0 || at != value.lastIndexOf('@') || at == value.length() - 1) { return false; }\n")
            .append("        for (String label : value.substring(at + 1).split(\"\\\\.\", -1)) { if (label.isEmpty()) { return false; } }\n")
            .append("        return true;\n    }\n\n")
            .append("    record NumericBounds(java.util.Optional<java.math.BigDecimal> minimum, java.util.Optional<java.math.BigDecimal> minimumExclusive, java.util.Optional<java.math.BigDecimal> maximum, java.util.Optional<java.math.BigDecimal> maximumExclusive) { }\n")
            .append("}\n");
        return builder.toString();
    }

    private void renderUnionAdapters(
        StringBuilder builder,
        PipelineTemplateTypeDefinition.UnionType union,
        PipelineV3GenerationPlan plan,
        String protoTypes
    ) {
        Map<String, PipelineIdlSnapshot.TypeVariantSnapshot> state = variantsByDiscriminator(union.name(), plan.idlState());
        List<PipelineTemplateTypeDefinition.Variant> variants = union.variants().values().stream()
            .sorted(Comparator.comparing(PipelineTemplateTypeDefinition.Variant::discriminator))
            .toList();
        builder.append("    public static ").append(protoTypes).append('.').append(union.name()).append(" toProto(")
            .append(union.name()).append(" value) {\n")
            .append("        if (value == null) { return null; }\n");
        for (PipelineTemplateTypeDefinition.Variant variant : variants) {
            PipelineIdlSnapshot.TypeVariantSnapshot variantState = requiredVariantState(union.name(), variant.discriminator(), state);
            String javaVariant = javaVariantTypeName(variant.discriminator());
            builder.append("        if (value instanceof ").append(union.name()).append('.').append(javaVariant).append(" variant) {\n")
                .append("            if (variant.value() == null) { throw new IllegalArgumentException(\"Pipeline union '")
                .append(javaStringLiteral(union.name())).append("' variant '").append(javaStringLiteral(variant.discriminator()))
                .append("' cannot carry a null payload.\"); }\n")
                .append("            return ").append(protoTypes).append('.').append(union.name()).append(".newBuilder().set")
                .append(javaSetter(variantState.protoName())).append('(')
                .append(toProtoExpression(variant.payload(), "variant.value()", plan.typeModel())).append(").build();\n")
                .append("        }\n");
        }
        builder.append("        throw new IllegalArgumentException(\"Unknown ").append(union.name())
            .append(" union variant: \" + value.getClass().getName());\n    }\n\n");

        builder.append("    public static ").append(union.name()).append(" fromProto(").append(protoTypes).append('.')
            .append(union.name()).append(" value) {\n")
            .append("        if (value == null) { return null; }\n")
            .append("        switch (value.getValueCase()) {\n");
        for (PipelineTemplateTypeDefinition.Variant variant : variants) {
            PipelineIdlSnapshot.TypeVariantSnapshot variantState = requiredVariantState(union.name(), variant.discriminator(), state);
            builder.append("            case ").append(variantState.protoName().toUpperCase(Locale.ROOT)).append(" -> { return new ")
                .append(union.name()).append('.').append(javaVariantTypeName(variant.discriminator())).append('(')
                .append(fromProtoExpression(variant.payload(), "value.get" + javaSetter(variantState.protoName()) + "()", plan.typeModel()))
                .append("); }\n");
        }
        builder.append("            default -> throw new IllegalArgumentException(\"Pipeline protobuf union '")
            .append(javaStringLiteral(union.name())).append("' has no selected variant.\");\n")
            .append("        }\n    }\n\n");
    }

    private void renderPayloadReferenceAdapters(StringBuilder builder, String protoTypes) {
        builder.append("    private static ").append(protoTypes).append(".PayloadReference toProtoPayloadReference(")
            .append(PAYLOAD_REFERENCE).append(" value) {\n")
            .append("        if (value == null) { return null; }\n")
            .append("        return ").append(protoTypes).append(".PayloadReference.newBuilder()\n")
            .append("            .setProvider(value.provider()).setContainer(value.container()).setKey(value.key())\n")
            .append("            .setContentType(value.contentType()).setCodec(value.codec()).setChecksum(value.checksum())\n")
            .append("            .setSizeBytes(value.sizeBytes()).setVersion(value.version()).putAllMetadata(value.metadata()).build();\n")
            .append("    }\n\n")
            .append("    private static ").append(PAYLOAD_REFERENCE).append(" fromProtoPayloadReference(")
            .append(protoTypes).append(".PayloadReference value) {\n")
            .append("        return new ").append(PAYLOAD_REFERENCE).append("(value.getProvider(), value.getContainer(), value.getKey(),\n")
            .append("            value.getContentType(), value.getCodec(), value.getChecksum(), value.getSizeBytes(), value.getVersion(),\n")
            .append("            value.getMetadataMap());\n    }\n\n");
    }

    private boolean usesPayloadReference(PipelineV3GenerationPlan plan) {
        return plan.typeModel().definitions().values().stream().anyMatch(definition ->
            definition instanceof PipelineTemplateTypeDefinition.RecordType record
                && record.fields().stream().anyMatch(field -> isPayloadReference(field.type(), plan.typeModel()))
            || definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper
                && isPayloadReference(wrapper.wraps(), plan.typeModel())
            || definition instanceof PipelineTemplateTypeDefinition.UnionType union
                && union.variants().values().stream().anyMatch(variant -> isPayloadReference(variant.payload(), plan.typeModel())));
    }

    private boolean isPayloadReference(PipelineTemplateTypeReference reference, PipelineTemplateTypeModel model) {
        return model.resolveAliases(reference) instanceof PipelineTemplateTypeReference.Scalar scalar
            && "payload_ref".equals(scalar.name());
    }

    private Map<String, PipelineIdlSnapshot.TypeFieldSnapshot> fieldsByName(String typeName, PipelineIdlSnapshot state) {
        PipelineIdlSnapshot.TypeSnapshot type = state.types().get(typeName);
        if (type == null) {
            throw new IllegalStateException("Missing compiler-owned IDL state for v3 type '" + typeName + "'.");
        }
        Map<String, PipelineIdlSnapshot.TypeFieldSnapshot> fields = new LinkedHashMap<>();
        type.fields().forEach(field -> fields.put(field.name(), field));
        return fields;
    }

    private PipelineIdlSnapshot.TypeFieldSnapshot requiredFieldState(
        String typeName,
        String fieldName,
        Map<String, PipelineIdlSnapshot.TypeFieldSnapshot> state
    ) {
        PipelineIdlSnapshot.TypeFieldSnapshot field = state.get(fieldName);
        if (field == null) {
            throw new IllegalStateException("Missing compiler-owned IDL state for v3 field '" + typeName + "." + fieldName + "'.");
        }
        return field;
    }

    private Map<String, PipelineIdlSnapshot.TypeVariantSnapshot> variantsByDiscriminator(
        String typeName,
        PipelineIdlSnapshot state
    ) {
        PipelineIdlSnapshot.TypeSnapshot type = state.types().get(typeName);
        if (type == null) {
            throw new IllegalStateException("Missing compiler-owned IDL state for v3 union '" + typeName + "'.");
        }
        Map<String, PipelineIdlSnapshot.TypeVariantSnapshot> variants = new LinkedHashMap<>();
        type.variants().forEach(variant -> variants.put(variant.discriminator(), variant));
        return variants;
    }

    private PipelineIdlSnapshot.TypeVariantSnapshot requiredVariantState(
        String unionName,
        String discriminator,
        Map<String, PipelineIdlSnapshot.TypeVariantSnapshot> state
    ) {
        PipelineIdlSnapshot.TypeVariantSnapshot variant = state.get(discriminator);
        if (variant == null) {
            throw new IllegalStateException("Missing compiler-owned IDL state for v3 union variant '"
                + unionName + "." + discriminator + "'.");
        }
        return variant;
    }

    private String resolveJavaType(PipelineTemplateTypeReference reference, PipelineTemplateTypeModel model) {
        PipelineTemplateTypeReference resolved = model.resolveAliases(reference);
        if (resolved instanceof PipelineTemplateTypeReference.Named named) {
            return named.name();
        }
        if (!(resolved instanceof PipelineTemplateTypeReference.Scalar scalar)) {
            throw new IllegalStateException("Version 3 Java domain target does not support generic or map type references.");
        }
        return switch (scalar.name()) {
            case "string" -> "String";
            case "bool" -> "Boolean";
            case "int32" -> "Integer";
            case "int64" -> "Long";
            case "float32" -> "Float";
            case "float64" -> "Double";
            case "decimal" -> "java.math.BigDecimal";
            case "uuid" -> "java.util.UUID";
            case "timestamp" -> "java.time.Instant";
            case "datetime" -> "java.time.LocalDateTime";
            case "date" -> "java.time.LocalDate";
            case "duration" -> "java.time.Duration";
            case "bytes" -> "com.google.protobuf.ByteString";
            case "currency" -> "java.util.Currency";
            case "uri" -> "java.net.URI";
            case "path" -> "java.nio.file.Path";
            case "payload_ref" -> PAYLOAD_REFERENCE;
            default -> throw new IllegalStateException("Unsupported version 3 Java scalar '" + scalar.name() + "'.");
        };
    }

    private String toProtoExpression(PipelineTemplateTypeReference reference, String expression, PipelineTemplateTypeModel model) {
        PipelineTemplateTypeReference resolved = model.resolveAliases(reference);
        if (resolved instanceof PipelineTemplateTypeReference.Named named) { return "toProto(" + expression + ")"; }
        String scalar = ((PipelineTemplateTypeReference.Scalar) resolved).name();
        return switch (scalar) {
            case "decimal" -> expression + ".toPlainString()";
            case "uuid", "timestamp", "datetime", "date", "duration", "currency", "uri", "path" -> expression + ".toString()";
            case "payload_ref" -> "toProtoPayloadReference(" + expression + ")";
            default -> expression;
        };
    }

    private String fromProtoExpression(PipelineTemplateTypeReference reference, String expression, PipelineTemplateTypeModel model) {
        PipelineTemplateTypeReference resolved = model.resolveAliases(reference);
        if (resolved instanceof PipelineTemplateTypeReference.Named named) { return "fromProto(" + expression + ")"; }
        String scalar = ((PipelineTemplateTypeReference.Scalar) resolved).name();
        return switch (scalar) {
            case "decimal" -> "new java.math.BigDecimal(" + expression + ")";
            case "uuid" -> "java.util.UUID.fromString(" + expression + ")";
            case "timestamp" -> "java.time.Instant.parse(" + expression + ")";
            case "datetime" -> "java.time.LocalDateTime.parse(" + expression + ")";
            case "date" -> "java.time.LocalDate.parse(" + expression + ")";
            case "duration" -> "java.time.Duration.parse(" + expression + ")";
            case "currency" -> "java.util.Currency.getInstance(" + expression + ")";
            case "uri" -> "java.net.URI.create(" + expression + ")";
            case "path" -> "java.nio.file.Path.of(" + expression + ")";
            case "payload_ref" -> "fromProtoPayloadReference(" + expression + ")";
            default -> expression;
        };
    }

    private String javaSetter(String protoName) {
        StringBuilder result = new StringBuilder();
        for (String part : protoName.split("_")) {
            if (!part.isEmpty()) {
                result.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
        }
        return result.toString();
    }

    private String javaVariantTypeName(String discriminator) {
        if (discriminator == null || discriminator.isBlank()) {
            throw new IllegalStateException("Version 3 union variant discriminator must not be null or blank.");
        }
        return Character.toUpperCase(discriminator.charAt(0)) + discriminator.substring(1);
    }

    private String javaStringLiteral(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
            .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private StringBuilder header(String domainPackage) {
        return new StringBuilder("// Generated by The Pipeline Framework. Do not edit.\n\npackage ")
            .append(domainPackage).append(";\n\n");
    }

    record RenderedSource(Path relativePath, String content) {
    }
}
