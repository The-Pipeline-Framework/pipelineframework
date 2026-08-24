/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.pipelineframework.proto;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

/** Renders the shared generated schema into pipeline-local protobuf contracts. */
final class PayloadReferenceProtoSchema {
    private PayloadReferenceProtoSchema() {
    }

    static void renderMessages(StringBuilder builder) {
        renderMessage(builder, PayloadReferenceStorageProto.ConnectorPayloadOrigin.getDescriptor());
        builder.append('\n');
        renderMessage(builder, PayloadReferenceStorageProto.PayloadReference.getDescriptor());
    }

    private static void renderMessage(StringBuilder builder, Descriptor descriptor) {
        builder.append("message ").append(descriptor.getName()).append(" {\n");
        for (FieldDescriptor field : descriptor.getFields()) {
            builder.append("  ");
            if (field.isRepeated() && !field.isMapField()) {
                builder.append("repeated ");
            }
            builder.append(field.isMapField() ? mapType(field) : type(field))
                .append(' ')
                .append(field.getName())
                .append(" = ")
                .append(field.getNumber())
                .append(";\n");
        }
        builder.append("}\n");
    }

    private static String mapType(FieldDescriptor field) {
        Descriptor entry = field.getMessageType();
        return "map<" + type(entry.findFieldByName("key")) + ", " + type(entry.findFieldByName("value")) + ">";
    }

    private static String type(FieldDescriptor field) {
        return switch (field.getType()) {
            case BOOL -> "bool";
            case BYTES -> "bytes";
            case DOUBLE -> "double";
            case ENUM -> field.getEnumType().getName();
            case FIXED32 -> "fixed32";
            case FIXED64 -> "fixed64";
            case FLOAT -> "float";
            case GROUP, MESSAGE -> field.getMessageType().getName();
            case INT32 -> "int32";
            case INT64 -> "int64";
            case SFIXED32 -> "sfixed32";
            case SFIXED64 -> "sfixed64";
            case SINT32 -> "sint32";
            case SINT64 -> "sint64";
            case STRING -> "string";
            case UINT32 -> "uint32";
            case UINT64 -> "uint64";
        };
    }
}
