/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.config.template;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Shared normalization helpers for template field semantics across runtime loader and proto generation.
 */
final class PipelineTemplateTypeMappings {

    private static final Set<String> BUILTIN_TYPES = Set.of(
        "string",
        "bool",
        "int32",
        "int64",
        "float32",
        "float64",
        "decimal",
        "uuid",
        "timestamp",
        "datetime",
        "date",
        "duration",
        "bytes",
        "currency",
        "uri",
        "path",
        "map");

    private static final Map<String, String> JAVA_TYPES = Map.ofEntries(
        Map.entry("string", "String"),
        Map.entry("bool", "Boolean"),
        Map.entry("int32", "Integer"),
        Map.entry("int64", "Long"),
        Map.entry("float32", "Float"),
        Map.entry("float64", "Double"),
        Map.entry("decimal", "BigDecimal"),
        Map.entry("uuid", "UUID"),
        Map.entry("timestamp", "Instant"),
        Map.entry("datetime", "LocalDateTime"),
        Map.entry("date", "LocalDate"),
        Map.entry("duration", "Duration"),
        Map.entry("bytes", "byte[]"),
        Map.entry("currency", "Currency"),
        Map.entry("uri", "URI"),
        Map.entry("path", "Path"));

    private static final Map<String, String> PROTO_TYPES = Map.ofEntries(
        Map.entry("string", "string"),
        Map.entry("bool", "bool"),
        Map.entry("int32", "int32"),
        Map.entry("int64", "int64"),
        Map.entry("float32", "float"),
        Map.entry("float64", "double"),
        Map.entry("decimal", "string"),
        Map.entry("uuid", "string"),
        Map.entry("timestamp", "string"),
        Map.entry("datetime", "string"),
        Map.entry("date", "string"),
        Map.entry("duration", "string"),
        Map.entry("bytes", "bytes"),
        Map.entry("currency", "string"),
        Map.entry("uri", "string"),
        Map.entry("path", "string"));
    private static final Set<String> MAP_KEY_CANONICAL_TYPES = Set.of("string", "bool", "int32", "int64");

    private PipelineTemplateTypeMappings() {
    }

    static boolean isBuiltinType(String value) {
        return value != null && BUILTIN_TYPES.contains(value.trim().toLowerCase(Locale.ROOT));
    }

    static boolean isMessageReferenceToken(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        String trimmed = value.trim();
        return Character.isUpperCase(trimmed.charAt(0)) && !trimmed.contains("<");
    }

    static boolean isLegacyListType(String value) {
        return value != null && value.startsWith("List<") && value.endsWith(">");
    }

    static boolean isLegacyMapType(String value) {
        return value != null && value.startsWith("Map<") && value.endsWith(">") && value.contains(",");
    }

    static String canonicalTypeForV2(String authoredType) {
        if (authoredType == null || authoredType.isBlank()) {
            return null;
        }
        String trimmed = authoredType.trim();
        if (isBuiltinType(trimmed)) {
            return trimmed.toLowerCase(Locale.ROOT);
        }
        if (isMessageReferenceToken(trimmed)) {
            return "message";
        }
        return null;
    }

    static String javaTypeForCanonical(String canonicalType, String messageRef, String keyType, String valueType, boolean repeated) {
        String resolved;
        if ("map".equals(canonicalType)) {
            resolved = "Map<" + javaTypeForSimpleToken(keyType, false) + ", " + javaTypeForSimpleToken(valueType, false) + ">";
        } else if ("message".equals(canonicalType)) {
            resolved = messageRef;
        } else {
            resolved = JAVA_TYPES.get(canonicalType);
        }
        if (resolved == null) {
            resolved = messageRef != null ? messageRef : "String";
        }
        if (repeated) {
            return "List<" + resolved + ">";
        }
        return resolved;
    }

    static String protoTypeForCanonical(String canonicalType, String messageRef, String keyType, String valueType) {
        if ("map".equals(canonicalType)) {
            return "map<" + protoScalarForToken(keyType, true) + ", " + protoScalarForToken(valueType, false) + ">";
        }
        if ("message".equals(canonicalType)) {
            return messageRef;
        }
        return PROTO_TYPES.getOrDefault(canonicalType, "string");
    }

    static PipelineTemplateField normalizeV2Field(PipelineTemplateField field, List<String> knownMessages) {
        String canonicalType = canonicalTypeForV2(field.type());
        if (canonicalType == null) {
            throw new IllegalStateException("Unknown v2 field type '" + field.type() + "' for field '" + field.name() + "'");
        }
        String messageRef = "message".equals(canonicalType) ? field.type() : null;
        if (messageRef != null && knownMessages != null && !knownMessages.contains(messageRef)) {
            throw new IllegalStateException("Unknown message reference '" + messageRef + "' for field '" + field.name() + "'");
        }
        if ("map".equals(canonicalType) && (field.keyType() == null || field.valueType() == null)) {
            throw new IllegalStateException("Map field '" + field.name() + "' must declare keyType and valueType");
        }
        if ("map".equals(canonicalType) && isMessageReferenceToken(field.keyType())) {
            throw new IllegalStateException("Map field '" + field.name() + "' cannot use a message type as keyType");
        }
        if ("map".equals(canonicalType)) {
            String keyCanonicalType = canonicalTypeForV2(field.keyType());
            if (keyCanonicalType == null || "message".equals(keyCanonicalType)) {
                throw new IllegalStateException(
                    "Map field '" + field.name() + "' declares unsupported keyType '" + field.keyType() + "'");
            }
            if ("float32".equals(keyCanonicalType) || "float64".equals(keyCanonicalType) || "bytes".equals(keyCanonicalType)) {
                throw new IllegalStateException(
                    "Map field '" + field.name() + "' keyType '" + field.keyType()
                        + "' is not allowed; protobuf map keys cannot be float or bytes");
            }
            if (!MAP_KEY_CANONICAL_TYPES.contains(keyCanonicalType)) {
                throw new IllegalStateException(
                    "Map field '" + field.name() + "' keyType '" + field.keyType()
                        + "' is not one of the allowed protobuf map key kinds: string, bool, int32, int64");
            }
        }
        if (!field.hasStableNumber()) {
            throw new IllegalStateException("Field '" + field.name() + "' must declare a positive number in version 2");
        }
        if ("map".equals(canonicalType) && field.repeated()) {
            throw new IllegalStateException("Map field '" + field.name() + "' cannot also be repeated");
        }

        String javaType = javaTypeForCanonical(canonicalType, messageRef, field.keyType(), field.valueType(), field.repeated());
        String defaultProto = protoTypeForCanonical(canonicalType, messageRef, field.keyType(), field.valueType());
        String overridden = resolveSafeProtoOverride(field, defaultProto);
        return new PipelineTemplateField(
            field.number(),
            field.name(),
            field.type(),
            canonicalType,
            messageRef,
            javaType,
            overridden,
            field.keyType(),
            field.valueType(),
            field.optional(),
            field.repeated(),
            field.deprecated(),
            field.since(),
            field.deprecatedSince(),
            field.comment(),
            field.overrides());
    }

    static PipelineTemplateField normalizeLegacyField(PipelineTemplateField field) {
        String declaredType = field.type();
        String canonicalType;
        String messageRef = null;
        String javaType = declaredType;
        String protoType = field.protoType();
        if (isLegacyListType(declaredType)) {
            canonicalType = canonicalForLegacySimple(listInnerType(declaredType));
            javaType = declaredType;
            protoType = protoScalarForToken(listInnerType(declaredType), false);
            return new PipelineTemplateField(
                null,
                field.name(),
                declaredType,
                canonicalType,
                null,
                javaType,
                protoType,
                null,
                null,
                false,
                true,
                false,
                null,
                null,
                null,
                null);
        }
        if (isLegacyMapType(declaredType)) {
            String keyType = mapParts(declaredType).get(0);
            String valueType = mapParts(declaredType).get(1);
            javaType = declaredType;
            protoType = "map<" + protoScalarForToken(keyType, true) + ", " + protoScalarForToken(valueType, false) + ">";
            return new PipelineTemplateField(
                null,
                field.name(),
                declaredType,
                "map",
                null,
                javaType,
                protoType,
                keyType,
                valueType,
                false,
                false,
                false,
                null,
                null,
                null,
                null);
        }
        canonicalType = canonicalForLegacySimple(declaredType);
        if ("message".equals(canonicalType)) {
            messageRef = declaredType;
            javaType = declaredType;
            protoType = protoType == null || protoType.isBlank() ? declaredType : protoType;
        } else {
            javaType = JAVA_TYPES.getOrDefault(canonicalType, declaredType);
            protoType = protoType == null || protoType.isBlank() ? PROTO_TYPES.getOrDefault(canonicalType, "string") : protoType;
        }
        return new PipelineTemplateField(
            null,
            field.name(),
            declaredType,
            canonicalType,
            messageRef,
            javaType,
            protoType,
            null,
            null,
            false,
            false,
            false,
            null,
            null,
            null,
            null);
    }

    private static String resolveSafeProtoOverride(PipelineTemplateField field, String defaultProto) {
        PipelineTemplateFieldOverrides overrides = field.overrides();
        if (overrides == null || overrides.proto() == null || overrides.proto().encoding() == null
            || overrides.proto().encoding().isBlank()) {
            return defaultProto;
        }
        String override = overrides.proto().encoding().trim();
        if (!override.equals(defaultProto)) {
            throw new IllegalStateException(
                "Unsafe protobuf override '" + override + "' for field '" + field.name()
                    + "'; allowed encoding is '" + defaultProto + "'");
        }
        return override;
    }

    private static String canonicalForLegacySimple(String javaType) {
        if (javaType == null || javaType.isBlank()) {
            return "string";
        }
        return switch (javaType) {
            case "String" -> "string";
            case "Boolean" -> "bool";
            case "Integer", "AtomicInteger" -> "int32";
            case "Long", "AtomicLong" -> "int64";
            case "Float" -> "float32";
            case "Double" -> "float64";
            case "BigDecimal" -> "decimal";
            case "UUID" -> "uuid";
            case "LocalDateTime", "OffsetDateTime", "ZonedDateTime" -> "datetime";
            case "Instant" -> "timestamp";
            case "LocalDate" -> "date";
            case "Duration", "Period" -> "duration";
            case "Currency" -> "currency";
            case "URI", "URL" -> "uri";
            case "Path", "File" -> "path";
            case "byte[]" -> "bytes";
            default -> isMessageReferenceToken(javaType) ? "message" : "string";
        };
    }

    private static String javaTypeForSimpleToken(String token, boolean keyType) {
        String canonical = canonicalForLegacySimple(token);
        if ("message".equals(canonical)) {
            return keyType ? "String" : token;
        }
        return JAVA_TYPES.getOrDefault(canonical, "String");
    }

    private static String protoScalarForToken(String token, boolean keyType) {
        String canonical = canonicalForLegacySimple(token);
        if ("message".equals(canonical)) {
            return keyType ? "string" : token;
        }
        return PROTO_TYPES.getOrDefault(canonical, "string");
    }

    private static String listInnerType(String type) {
        return type.substring(5, type.length() - 1).trim();
    }

    private static List<String> mapParts(String type) {
        String inner = type.substring(4, type.length() - 1);
        String[] parts = inner.split(",");
        String key = parts.length > 0 ? parts[0].trim() : "String";
        String value = parts.length > 1 ? parts[1].trim() : "String";
        return List.of(key, value);
    }
}
