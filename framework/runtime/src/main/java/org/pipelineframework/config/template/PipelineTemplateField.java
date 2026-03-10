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

import java.util.Objects;

/**
 * Normalized field metadata from the pipeline template configuration.
 *
 * @param number stable field number for v2 messages; null for legacy v1 fields
 * @param name field name
 * @param type authored type token (legacy Java type for v1, semantic type or message ref for v2)
 * @param canonicalType normalized semantic kind used by the compiler
 * @param messageRef referenced message name when canonicalType is {@code message}
 * @param javaType resolved Java binding type
 * @param protoType resolved protobuf field type without repeated/optional modifiers
 * @param keyType authored key type for map fields
 * @param valueType authored value type for map fields
 * @param optional whether the field is optional
 * @param repeated whether the field is repeated
 * @param deprecated whether the field is deprecated
 * @param since version metadata for field introduction
 * @param deprecatedSince version metadata for field deprecation
 * @param comment optional field comment
 * @param overrides optional override metadata
 */
public record PipelineTemplateField(
    Integer number,
    String name,
    String type,
    String canonicalType,
    String messageRef,
    String javaType,
    String protoType,
    String keyType,
    String valueType,
    boolean optional,
    boolean repeated,
    boolean deprecated,
    String since,
    String deprecatedSince,
    String comment,
    PipelineTemplateFieldOverrides overrides
) {
    public PipelineTemplateField {
        name = normalize(name);
        type = normalize(type);
        canonicalType = normalize(canonicalType);
        messageRef = normalize(messageRef);
        javaType = normalize(javaType);
        protoType = normalize(protoType);
        keyType = normalize(keyType);
        valueType = normalize(valueType);
        since = normalize(since);
        deprecatedSince = normalize(deprecatedSince);
        comment = normalize(comment);
    }

    /**
     * Backward-compatible constructor retained for legacy tests and callers that still model v1 fields.
     *
     * @param name field name
     * @param type authored/legacy type token
     * @param protoType authored/legacy protobuf field type
     */
    public PipelineTemplateField(String name, String type, String protoType) {
        this(null, name, type, null, null, null, protoType, null, null, false, false, false, null, null, null, null);
    }

    public boolean isMap() {
        return "map".equals(canonicalType);
    }

    public boolean isMessageReference() {
        return "message".equals(canonicalType) && messageRef != null;
    }

    public boolean hasStableNumber() {
        return number != null && number > 0;
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    public PipelineTemplateField withBindings(String resolvedJavaType, String resolvedProtoType) {
        return new PipelineTemplateField(
            number,
            name,
            type,
            canonicalType,
            messageRef,
            resolvedJavaType,
            resolvedProtoType,
            keyType,
            valueType,
            optional,
            repeated,
            deprecated,
            since,
            deprecatedSince,
            comment,
            overrides);
    }

    public PipelineTemplateField withCanonicalType(String resolvedCanonicalType, String resolvedMessageRef) {
        return new PipelineTemplateField(
            number,
            name,
            type,
            Objects.requireNonNullElse(resolvedCanonicalType, canonicalType),
            resolvedMessageRef,
            javaType,
            protoType,
            keyType,
            valueType,
            optional,
            repeated,
            deprecated,
            since,
            deprecatedSince,
            comment,
            overrides);
    }
}
