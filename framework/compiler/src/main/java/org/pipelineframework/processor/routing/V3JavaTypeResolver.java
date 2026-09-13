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

package org.pipelineframework.processor.routing;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateDialect;
import org.pipelineframework.config.template.PipelineTemplateJavaScalarTypes;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;

/** Resolves compiler-owned v3 semantic type identities to their canonical generated Java types. */
public final class V3JavaTypeResolver {
    private final PipelineTemplateConfig config;
    private final PipelineTemplateJavaScalarTypes javaScalarTypes = new PipelineTemplateJavaScalarTypes();

    public V3JavaTypeResolver(PipelineTemplateConfig config) {
        if (config == null || config.dialect() != PipelineTemplateDialect.V3) {
            throw new IllegalArgumentException("V3JavaTypeResolver requires a v3 pipeline template");
        }
        this.config = config;
    }

    /** Resolve a named semantic type, transparently following aliases. */
    public Optional<ClassName> resolve(String semanticType) {
        if (semanticType == null || semanticType.isBlank()) {
            return Optional.empty();
        }
        PipelineTemplateTypeReference resolved = config.typeModel().resolveAliases(
            new PipelineTemplateTypeReference.Named(semanticType));
        if (resolved instanceof PipelineTemplateTypeReference.Named named
            && config.typeModel().definition(named.name()).isPresent()) {
            Optional<String> bound = config.typeModel().javaTypeBinding(named.name());
            if (bound.isPresent()) {
                return Optional.of(ClassName.bestGuess(bound.orElseThrow()));
            }
            return Optional.of(ClassName.get(config.basePackage() + ".domain", named.name()));
        }
        if (resolved instanceof PipelineTemplateTypeReference.Scalar scalar) {
            return Optional.of(className(javaScalarTypes.typeName(scalar.name())));
        }
        return Optional.empty();
    }

    /** Canonical runtime types for every named v3 definition, including transparent aliases. */
    public Map<String, ClassName> allNamedTypes() {
        Map<String, ClassName> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateTypeDefinition> entry : config.typeModel().definitions().entrySet()) {
            resolve(entry.getKey()).ifPresent(type -> resolved.put(entry.getKey(), type));
            if (!(entry.getValue() instanceof PipelineTemplateTypeDefinition.AliasType)) {
                resolved.putIfAbsent(entry.getValue().name(),
                    ClassName.get(config.basePackage() + ".domain", entry.getValue().name()));
            }
        }
        return Map.copyOf(resolved);
    }

    /** Finds the semantic identity represented by a canonical generated Java class. */
    public Optional<String> semanticType(ClassName javaType) {
        if (javaType == null) {
            return Optional.empty();
        }
        Optional<String> bound = config.typeModel().javaTypeBindings().entrySet().stream()
            .filter(entry -> javaType.canonicalName().equals(entry.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();
        if (bound.isPresent()) {
            return bound;
        }
        String prefix = config.basePackage() + ".domain.";
        if (!javaType.canonicalName().startsWith(prefix)) {
            return Optional.empty();
        }
        String candidate = javaType.canonicalName().substring(prefix.length());
        return config.typeModel().definition(candidate).map(PipelineTemplateTypeDefinition::name);
    }

    private ClassName className(String javaType) {
        return switch (javaType) {
            case "String", "Boolean", "Integer", "Long", "Float", "Double" ->
                ClassName.get("java.lang", javaType);
            default -> ClassName.bestGuess(javaType);
        };
    }
}
