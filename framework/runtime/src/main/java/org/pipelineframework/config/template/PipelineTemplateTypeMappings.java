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

    /**
     * Prevents instantiation of this utility class which only contains static helpers.
     */
    private PipelineTemplateTypeMappings() {
    }

    /**
     * Determine whether a string corresponds to a supported builtin type name.
     *
     * @param value the string to test; may be null
     * @return `true` if `value`, after trimming and case-insensitive comparison, matches a supported builtin type name, `false` otherwise
     */
    static boolean isBuiltinType(String value) {
        return value != null && BUILTIN_TYPES.contains(value.trim().toLowerCase(Locale.ROOT));
    }

    /**
     * Determines whether a string is a message reference token.
     *
     * @param value the string to test
     * @return `true` if `value` is non-blank, begins with an uppercase character, and does not contain `<`, `false` otherwise
     */
    static boolean isMessageReferenceToken(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        String trimmed = value.trim();
        return Character.isUpperCase(trimmed.charAt(0)) && !trimmed.contains("<");
    }

    /**
     * Checks whether a string represents a legacy List<T> type declaration.
     *
     * @param value the string to test for legacy List<T> syntax (may be null)
     * @return `true` if the input is non-null and starts with "List<" and ends with ">", `false` otherwise
     */
    static boolean isLegacyListType(String value) {
        return value != null && value.startsWith("List<") && value.endsWith(">");
    }

    /**
     * Determines whether a string represents a legacy Java Map<K,V> type declaration.
     *
     * @param value the type string to check (may be null)
     * @return `true` if the string has the form "Map<...,...>" (starts with "Map<", ends with ">", and contains a comma), `false` otherwise
     */
    static boolean isLegacyMapType(String value) {
        return value != null && value.startsWith("Map<") && value.endsWith(">") && value.contains(",");
    }

    /**
     * Determine the canonical type name for a V2 authored field type.
     *
     * <p>Maps a non-blank authored type to a canonical representation: built-in types are returned
     * in lowercase, message reference tokens are mapped to "message", and blank or unrecognized
     * types map to {@code null}.
     *
     * @param authoredType the authored type string (may be null or blank)
     * @return the canonical type ("message" or a lowercase builtin) or {@code null} if none applies
     */
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

    /**
     * Resolve the Java type name for a canonical template type.
     *
     * @param canonicalType the canonical type name (e.g., "map", "message", or a builtin canonical)
     * @param messageRef the message type name to use for "message" canonical types or as a fallback
     * @param keyType the declared key token for map types (used to derive the map's key Java type)
     * @param valueType the declared value token for map types (used to derive the map's value Java type)
     * @param repeated if true, wrap the resolved type in a List (e.g., List&lt;T&gt;)
     * @return the resolved Java type name; for maps returns a parameterized Map&lt;Key, Value&gt;, for message returns the messageRef, otherwise a mapped builtin Java type, and if `repeated` is true the result is wrapped as List&lt;...&gt;
     */
    static String javaTypeForCanonical(String canonicalType, String messageRef, String keyType, String valueType, boolean repeated) {
        String resolved;
        if ("map".equals(canonicalType)) {
            resolved = "Map<" + javaTypeForTokenV2(keyType) + ", " + javaTypeForTokenV2(valueType) + ">";
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

    /**
     * Resolve the Protobuf type string for a canonical field type.
     *
     * @param canonicalType the canonical type name (e.g., "map", "message", or a builtin canonical)
     * @param messageRef the Protobuf message type name to use when canonicalType is "message"
     * @param keyType the declared key token for map types (used to derive the key scalar)
     * @param valueType the declared value token for map types (used to derive the value scalar)
     * @return `map<keyProto, valueProto>` for canonicalType "map", the `messageRef` for canonicalType "message", or the Protobuf scalar type for other canonical types; defaults to `string` if no mapping exists
     */
    static String protoTypeForCanonical(String canonicalType, String messageRef, String keyType, String valueType) {
        if ("map".equals(canonicalType)) {
            return "map<" + protoScalarForTokenV2(keyType, true) + ", " + protoScalarForTokenV2(valueType, false) + ">";
        }
        if ("message".equals(canonicalType)) {
            return messageRef;
        }
        return PROTO_TYPES.getOrDefault(canonicalType, "string");
    }

    /**
     * Validate and normalize a v2 pipeline template field, producing a canonical field representation.
     *
     * @param field the v2 field to validate and normalize
     * @param knownMessages optional list of allowed message names; if non-null, message references must be present in this list
     * @return a new PipelineTemplateField populated with canonical type, resolved Java type, resolved Protobuf type (possibly overridden), and preserved metadata
     * @throws IllegalStateException if the field's type is unknown, the message reference is unknown, a map field lacks or has invalid key/value types, a map key uses a disallowed kind (float/bytes or not one of string/bool/int32/int64), the field lacks a stable positive number, a map is marked repeated, or a protobuf override is unsafe
     */
    static PipelineTemplateField normalizeV2Field(PipelineTemplateField field, List<String> knownMessages) {
        if (field.name() == null || field.name().isBlank()) {
            throw new IllegalStateException("Field name must not be blank for v2 type '" + field.type() + "'");
        }
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
            String valueCanonicalType = canonicalTypeForV2(field.valueType());
            if (valueCanonicalType == null) {
                throw new IllegalStateException(
                    "Map field '" + field.name() + "' declares unsupported valueType '" + field.valueType() + "'");
            }
            if ("map".equals(valueCanonicalType)) {
                throw new IllegalStateException(
                    "Map field '" + field.name() + "' cannot use map as valueType");
            }
            if ("message".equals(valueCanonicalType)
                && knownMessages != null
                && !knownMessages.contains(field.valueType())) {
                throw new IllegalStateException(
                    "Map field '" + field.name() + "' references unknown message valueType '" + field.valueType() + "'");
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

    /**
     * Normalize a legacy-declared PipelineTemplateField into the canonical internal representation.
     *
     * <p>Handles legacy Java-style type declarations (e.g., "List<T>", "Map<K,V>", primitive or boxed
     * types, and message type names) and returns a new field with canonicalType, javaType, protoType,
     * messageRef, keyType/valueType, and repeated flag populated according to the legacy declaration.
     *
     * @param field the legacy-declared field to normalize; its declared type and optional protoType
     *              override are used to derive the normalized values
     * @return a new PipelineTemplateField with normalized type information:
     *         - list declarations produce a repeated field with the inner canonical type and proto scalar
     *         - map declarations produce a canonical "map" field with keyType/valueType and a map proto type
     *         - message-like types set messageRef and default the protoType to the message name when unset
     *         - builtin/primitive types are mapped to their canonical, Java, and proto equivalents with sensible defaults
     */
    static PipelineTemplateField normalizeLegacyField(PipelineTemplateField field) {
        String declaredType = field.type();
        String canonicalType;
        String messageRef = null;
        String javaType = declaredType;
        String protoType = field.protoType();
        if (isLegacyListType(declaredType)) {
            String innerType = listInnerType(declaredType);
            canonicalType = canonicalForLegacySimple(innerType);
            if ("message".equals(canonicalType)) {
                messageRef = innerType;
            }
            javaType = declaredType;
            protoType = protoScalarForToken(innerType, false);
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
                true,
                false,
                null,
                null,
                null,
                null);
        }
        if (isLegacyMapType(declaredType)) {
            List<String> parts = mapParts(declaredType);
            String keyType = parts.get(0);
            String valueType = parts.get(1);
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

    /**
     * Validates and returns the protobuf encoding for the field, applying a safe override if present.
     *
     * If the field has no proto encoding override, the provided default is returned. If an override is present,
     * it must exactly match the default; otherwise an IllegalStateException is thrown.
     *
     * @param field the template field to inspect for a proto encoding override
     * @param defaultProto the default protobuf encoding to use when no override is provided
     * @return the effective protobuf encoding (either the default or the validated override)
     * @throws IllegalStateException if the field provides an override that differs from the default
     */
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

    /**
     * Map a legacy Java type token to the canonical builtin type name used by the template system.
     *
     * <p>Null or blank inputs are treated as {@code "string"}.
     *
     * @param javaType the simple Java type token (for example {@code "String"}, {@code "Integer"},
     *                 or a message reference like {@code "MyMessage"})
     * @return the canonical type name such as {@code "string"}, {@code "bool"}, {@code "int32"},
     *         {@code "int64"}, {@code "float32"}, {@code "float64"}, {@code "decimal"},
     *         {@code "uuid"}, {@code "datetime"}, {@code "timestamp"}, {@code "date"},
     *         {@code "duration"}, {@code "currency"}, {@code "uri"}, {@code "path"},
     *         {@code "bytes"}, or {@code "message"} when the token appears to be a message reference;
     *         defaults to {@code "string"} when the token is unrecognized.
     */
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

    /**
     * Resolve a legacy type token to the Java type name used in generated or runtime code.
     *
     * @param token a legacy type token (for example `"String"`, `"Integer"`, or a message reference like `"MyMessage"`)
     * @param keyType true if the token is being used as a map key (map keys that reference messages become `String`)
     * @return the Java type name for the given token; for message tokens returns `String` when used as a map key, otherwise returns the token itself
     */
    private static String javaTypeForSimpleToken(String token, boolean keyType) {
        String canonical = canonicalForLegacySimple(token);
        if ("message".equals(canonical)) {
            return keyType ? "String" : token;
        }
        return JAVA_TYPES.getOrDefault(canonical, "String");
    }

    /**
     * Maps a legacy Java-type token to the corresponding Protobuf scalar type name for use in proto fields.
     *
     * @param token   a legacy Java type token or a message reference identifier
     * @param keyType true if the resulting scalar is intended for a protobuf map key
     * @return the Protobuf scalar type name (for example "int32", "string"); defaults to "string" when unknown or when a message token is used as a map key
     */
    private static String protoScalarForToken(String token, boolean keyType) {
        String canonical = canonicalForLegacySimple(token);
        if ("message".equals(canonical)) {
            return keyType ? "string" : token;
        }
        return PROTO_TYPES.getOrDefault(canonical, "string");
    }

    private static String javaTypeForTokenV2(String token) {
        String canonical = canonicalTypeForV2(token);
        if (canonical == null || "message".equals(canonical)) {
            return token;
        }
        return JAVA_TYPES.getOrDefault(canonical, token);
    }

    private static String protoScalarForTokenV2(String token, boolean keyType) {
        String canonical = canonicalTypeForV2(token);
        if (canonical == null) {
            return keyType ? "string" : token;
        }
        if ("message".equals(canonical)) {
            return keyType ? "string" : token;
        }
        return PROTO_TYPES.getOrDefault(canonical, keyType ? "string" : token);
    }

    /**
     * Extracts the inner type token from a legacy List<T> declaration.
     *
     * @param type the legacy list declaration in the form "List<InnerType>"
     * @return the trimmed inner type token (the text between '<' and '>')
     */
    private static String listInnerType(String type) {
        return type.substring(5, type.length() - 1).trim();
    }

    /**
     * Extracts the key and value type tokens from a legacy Map<K,V> declaration string.
     *
     * @param type the legacy Map declaration (for example "Map<String, Integer>"); must start with "Map<" and end with ">"
     * @return a list of two strings: the first element is the key type token and the second element is the value type token;
     *         if a part is missing it defaults to "String"
     */
    private static List<String> mapParts(String type) {
        String inner = type.substring(4, type.length() - 1);
        int depth = 0;
        int splitIndex = -1;
        for (int i = 0; i < inner.length(); i++) {
            char ch = inner.charAt(i);
            if (ch == '<') {
                depth++;
            } else if (ch == '>') {
                depth = Math.max(0, depth - 1);
            } else if (ch == ',' && depth == 0) {
                splitIndex = i;
                break;
            }
        }
        String key = splitIndex >= 0 ? inner.substring(0, splitIndex).trim() : inner.trim();
        String value = splitIndex >= 0 ? inner.substring(splitIndex + 1).trim() : "String";
        if (key.isBlank()) {
            key = "String";
        }
        if (value.isBlank()) {
            value = "String";
        }
        return List.of(key, value);
    }
}
