/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.pipelineframework.proto;

import java.util.*;

import org.pipelineframework.config.template.*;

/** Renders the shared pipeline types proto without redefining template semantics. */
final class PipelineTypesProtoRenderer {

    String renderV2(
        String basePackage,
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions
    ) {
        Map<String, PipelineTemplateMessage> safeMessages = messages == null ? Map.of() : new LinkedHashMap<>(messages);
        Map<String, PipelineTemplateUnion> safeUnions = unions == null ? Map.of() : new LinkedHashMap<>(unions);
        StringBuilder builder = header(basePackage);
        boolean first = true;
        if (usesPayloadReference(safeMessages)) {
            renderPayloadReferenceMessage(builder);
            first = false;
        }
        List<String> messageNames = new ArrayList<>(safeMessages.keySet());
        Collections.sort(messageNames);
        for (String messageName : messageNames) {
            if (!first) { builder.append('\n'); }
            renderV2Message(builder, safeMessages.get(messageName));
            first = false;
        }
        List<String> unionNames = new ArrayList<>(safeUnions.keySet());
        Collections.sort(unionNames);
        for (String unionName : unionNames) {
            if (!first) { builder.append('\n'); }
            renderV2Union(builder, safeUnions.get(unionName));
            first = false;
        }
        return builder.toString();
    }

    String renderV3(String basePackage, PipelineTemplateTypeModel model, PipelineIdlSnapshot state) {
        StringBuilder builder = header(basePackage);
        boolean first = true;
        if (usesV3PayloadReference(model)) {
            renderPayloadReferenceMessage(builder);
            first = false;
        }
        List<String> names = new ArrayList<>(model.definitions().keySet());
        Collections.sort(names);
        for (String name : names) {
            PipelineTemplateTypeDefinition definition = model.definitions().get(name);
            if (definition instanceof PipelineTemplateTypeDefinition.AliasType) {
                continue;
            }
            if (!first) { builder.append('\n'); }
            PipelineIdlSnapshot.TypeSnapshot typeState = state.types().get(name);
            if (typeState == null) {
                throw new IllegalStateException("Missing compiler-owned IDL state for v3 type '" + name + "'.");
            }
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                renderV3Record(builder, record, typeState, model);
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                renderV3Wrapper(builder, wrapper, model);
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                renderV3Union(builder, union, typeState, model);
            }
            first = false;
        }
        return builder.toString();
    }

    private StringBuilder header(String basePackage) {
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("package ").append(basePackage).append(";\n\n");
        builder.append("option java_package = \"").append(basePackage).append(".grpc\";\n\n");
        builder.append("option java_outer_classname = \"PipelineTypes\";\n\n");
        return builder;
    }

    private void renderV3Record(
        StringBuilder builder,
        PipelineTemplateTypeDefinition.RecordType record,
        PipelineIdlSnapshot.TypeSnapshot state,
        PipelineTemplateTypeModel model
    ) {
        builder.append("message ").append(record.name()).append(" {\n");
        renderReservations(builder, state.reservedNumbers(), state.reservedNames());
        Map<String, PipelineTemplateTypeDefinition.Field> fields = new LinkedHashMap<>();
        record.fields().forEach(field -> fields.put(field.name(), field));
        for (PipelineIdlSnapshot.TypeFieldSnapshot field : state.fields().stream()
            .sorted(Comparator.comparingInt(PipelineIdlSnapshot.TypeFieldSnapshot::number)).toList()) {
            PipelineTemplateTypeDefinition.Field definition = fields.get(field.name());
            if (definition == null) {
                throw new IllegalStateException("IDL state references removed v3 field '" + record.name() + "." + field.name() + "'.");
            }
            String protoType = protoType(definition.type(), model);
            builder.append("  ");
            if (supportsExplicitPresence(definition.type(), model)) { builder.append("optional "); }
            builder.append(protoType).append(' ').append(field.protoName()).append(" = ").append(field.number()).append(";\n");
        }
        builder.append("}\n");
    }

    private void renderV3Wrapper(
        StringBuilder builder,
        PipelineTemplateTypeDefinition.WrapperType wrapper,
        PipelineTemplateTypeModel model
    ) {
        builder.append("message ").append(wrapper.name()).append(" {\n  ");
        if (supportsExplicitPresence(wrapper.wraps(), model)) { builder.append("optional "); }
        builder.append(protoType(wrapper.wraps(), model)).append(" value = 1;\n}\n");
    }

    private void renderV3Union(
        StringBuilder builder,
        PipelineTemplateTypeDefinition.UnionType union,
        PipelineIdlSnapshot.TypeSnapshot state,
        PipelineTemplateTypeModel model
    ) {
        builder.append("message ").append(union.name()).append(" {\n");
        renderReservations(builder, state.reservedNumbers(), state.reservedNames());
        builder.append("  oneof value {\n");
        Map<String, PipelineTemplateTypeDefinition.Variant> variants = union.variants();
        for (PipelineIdlSnapshot.TypeVariantSnapshot variant : state.variants().stream()
            .sorted(Comparator.comparingInt(PipelineIdlSnapshot.TypeVariantSnapshot::number)).toList()) {
            PipelineTemplateTypeDefinition.Variant definition = variants.get(variant.discriminator());
            if (definition == null) {
                throw new IllegalStateException("IDL state references removed v3 union variant '" + union.name() + "."
                    + variant.discriminator() + "'.");
            }
            builder.append("    ").append(protoType(definition.payload(), model)).append(' ').append(variant.protoName())
                .append(" = ").append(variant.number()).append(";\n");
        }
        builder.append("  }\n}\n");
    }

    private void renderReservations(StringBuilder builder, List<Integer> numbers, List<String> names) {
        if (!numbers.isEmpty()) {
            builder.append("  reserved ").append(numbers.stream().sorted().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(", "))).append(";\n");
        }
        if (!names.isEmpty()) {
            builder.append("  reserved ").append(names.stream().sorted().map(name -> "\"" + name + "\"")
                .collect(java.util.stream.Collectors.joining(", "))).append(";\n");
        }
    }

    static String protoType(PipelineTemplateTypeReference reference, PipelineTemplateTypeModel model) {
        PipelineTemplateTypeReference resolved = model.resolveAliases(reference);
        if (resolved instanceof PipelineTemplateTypeReference.Named named) { return named.name(); }
        String scalar = resolved.name();
        return switch (scalar) {
            case "bool", "int32", "int64" -> scalar;
            case "float32" -> "float";
            case "float64" -> "double";
            case "bytes" -> "bytes";
            case "payload_ref" -> "PayloadReference";
            default -> "string";
        };
    }

    private boolean supportsExplicitPresence(PipelineTemplateTypeReference reference, PipelineTemplateTypeModel model) {
        PipelineTemplateTypeReference resolved = model.resolveAliases(reference);
        return resolved instanceof PipelineTemplateTypeReference.Scalar && !"payload_ref".equals(resolved.name());
    }

    private boolean usesV3PayloadReference(PipelineTemplateTypeModel model) {
        return model.definitions().values().stream().anyMatch(definition -> {
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                return record.fields().stream().anyMatch(field -> "PayloadReference".equals(protoType(field.type(), model)));
            }
            if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                return "PayloadReference".equals(protoType(wrapper.wraps(), model));
            }
            if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                return union.variants().values().stream().anyMatch(variant -> "PayloadReference".equals(protoType(variant.payload(), model)));
            }
            return false;
        });
    }

    private void renderV2Message(StringBuilder builder, PipelineTemplateMessage message) {
        builder.append("message ").append(message.name()).append(" {\n");
        PipelineTemplateReserved reserved = message.reserved();
        if (reserved != null) { renderV2Reservations(builder, reserved); }
        List<PipelineTemplateField> fields = message.fields() == null ? List.of() : message.fields();
        for (PipelineTemplateField field : fields) { if (field != null) { renderV2Field(builder, field); } }
        builder.append("}\n");
    }

    private void renderV2Reservations(StringBuilder builder, PipelineTemplateReserved reserved) {
        if (reserved.numbers() != null && !reserved.numbers().isEmpty()) {
            builder.append("  reserved ");
            for (int i = 0; i < reserved.numbers().size(); i++) {
                if (i > 0) { builder.append(", "); }
                builder.append(reserved.numbers().get(i));
            }
            builder.append(";\n");
        }
        if (reserved.names() != null && !reserved.names().isEmpty()) {
            builder.append("  reserved ");
            for (int i = 0; i < reserved.names().size(); i++) {
                if (i > 0) { builder.append(", "); }
                builder.append('\"').append(reserved.names().get(i)).append('\"');
            }
            builder.append(";\n");
        }
    }

    private void renderV2Union(StringBuilder builder, PipelineTemplateUnion union) {
        builder.append("message ").append(union.name()).append(" {\n  oneof outcome {\n");
        for (PipelineTemplateUnionVariant variant : union.variants().values().stream()
            .sorted(Comparator.comparingInt(PipelineTemplateUnionVariant::number)).toList()) {
            builder.append("    ").append(v2UnionVariantType(variant.type())).append(' ').append(toProtoFieldName(variant.name()))
                .append(" = ").append(variant.number()).append(";\n");
        }
        builder.append("  }\n}\n");
    }

    private String v2UnionVariantType(String type) {
        return switch (type) {
            case "bool", "int32", "int64", "string" -> type;
            case "float32" -> "float";
            case "float64" -> "double";
            case "bytes" -> "bytes";
            case "decimal", "uuid", "timestamp", "datetime", "date", "duration", "currency", "uri", "path" -> "string";
            case "payload_ref" -> "PayloadReference";
            default -> type;
        };
    }

    private void renderV2Field(StringBuilder builder, PipelineTemplateField field) {
        if (field.comment() != null && !field.comment().isBlank()) {
            for (String rawLine : field.comment().split("\\R")) {
                if (rawLine != null && !rawLine.trim().isEmpty()) { builder.append("  // ").append(rawLine.trim()).append('\n'); }
            }
        }
        builder.append("  ");
        if (field.repeated()) { builder.append("repeated "); }
        else if (!field.isMap() && !field.isMessageReference() && !"payload_ref".equals(field.canonicalType())) { builder.append("optional "); }
        builder.append(field.protoType()).append(' ').append(field.name()).append(" = ").append(field.number());
        if (field.deprecated()) { builder.append(" [deprecated = true]"); }
        builder.append(";\n");
    }

    private boolean usesPayloadReference(Map<String, PipelineTemplateMessage> messages) {
        return messages.values().stream().filter(message -> message != null && message.fields() != null)
            .flatMap(message -> message.fields().stream())
            .anyMatch(field -> field != null && "payload_ref".equals(field.canonicalType()));
    }

    private void renderPayloadReferenceMessage(StringBuilder builder) {
        builder.append("message PayloadReference {\n");
        builder.append("  string provider = 1;\n  string container = 2;\n  string key = 3;\n");
        builder.append("  string content_type = 4;\n  string codec = 5;\n  string checksum = 6;\n");
        builder.append("  int64 size_bytes = 7;\n  string version = 8;\n  map<string, string> metadata = 9;\n}\n");
    }

    private String toProtoFieldName(String input) {
        if (input == null || input.isBlank()) { return ""; }
        StringBuilder builder = new StringBuilder();
        char previous = 0;
        for (int i = 0; i < input.length(); i++) {
            char current = input.charAt(i);
            if (!Character.isLetterOrDigit(current)) {
                if (!builder.isEmpty() && builder.charAt(builder.length() - 1) != '_') { builder.append('_'); }
            } else {
                if (Character.isUpperCase(current) && i > 0 && previous != 0 && previous != '_'
                    && Character.isLowerCase(previous)) { builder.append('_'); }
                builder.append(Character.toLowerCase(current));
            }
            previous = current;
        }
        return builder.toString().replaceAll("^_+|_+$", "");
    }
}
