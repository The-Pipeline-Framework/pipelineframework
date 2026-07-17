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

import java.util.*;

/**
 * Immutable semantic type graph shared by v3 consumers.
 *
 * <p>Aliases are transparent during assignability checks; wrappers retain their named identity.
 * A union exposes membership only. It does not create routing edges or runtime conversions.</p>
 */
public final class PipelineTemplateTypeModel {
    private final Map<String, PipelineTemplateTypeDefinition> definitions;

    public PipelineTemplateTypeModel(Map<String, PipelineTemplateTypeDefinition> definitions) {
        Map<String, PipelineTemplateTypeDefinition> copy = definitions == null
            ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(definitions));
        validate(copy);
        this.definitions = copy;
    }

    public static PipelineTemplateTypeModel empty() {
        return new PipelineTemplateTypeModel(Map.of());
    }

    public static PipelineTemplateTypeModel fromLegacy(
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions
    ) {
        Map<String, PipelineTemplateTypeDefinition> definitions = new LinkedHashMap<>();
        if (messages != null) {
            messages.forEach((name, message) -> definitions.put(name,
                new PipelineTemplateTypeDefinition.RecordType(name, message.fields().stream()
                    .map(field -> new PipelineTemplateTypeDefinition.Field(field.name(), legacyReference(field)))
                    .toList())));
        }
        if (unions != null) {
            unions.forEach((name, union) -> {
                Map<String, PipelineTemplateTypeDefinition.Variant> variants = new LinkedHashMap<>();
                union.variants().forEach((discriminator, variant) -> variants.put(discriminator,
                    new PipelineTemplateTypeDefinition.Variant(discriminator,
                        legacyReference(variant.type()))));
                definitions.put(name, new PipelineTemplateTypeDefinition.UnionType(name, variants));
            });
        }
        return new PipelineTemplateTypeModel(definitions);
    }

    private static PipelineTemplateTypeReference legacyReference(PipelineTemplateField field) {
        if (field.isMap()) {
            return new PipelineTemplateTypeReference.MapType(
                new PipelineTemplateTypeReference.Scalar(field.keyType()), legacyReference(field.valueType()));
        }
        if (field.messageRef() != null && !field.messageRef().isBlank()) {
            return new PipelineTemplateTypeReference.Named(field.messageRef());
        }
        return new PipelineTemplateTypeReference.Scalar(field.canonicalType());
    }

    private static PipelineTemplateTypeReference legacyReference(String type) {
        return PipelineTemplateTypeMappings.isV3ScalarType(type)
            ? new PipelineTemplateTypeReference.Scalar(type)
            : new PipelineTemplateTypeReference.Named(type);
    }

    public Map<String, PipelineTemplateTypeDefinition> definitions() {
        return definitions;
    }

    public Optional<PipelineTemplateTypeDefinition> definition(String name) {
        return Optional.ofNullable(definitions.get(name));
    }

    public boolean contains(String name) {
        return definitions.containsKey(name);
    }

    public PipelineTemplateTypeReference resolveAliases(PipelineTemplateTypeReference reference) {
        PipelineTemplateTypeReference current = reference;
        while (current instanceof PipelineTemplateTypeReference.Named named) {
            PipelineTemplateTypeDefinition definition = definitions.get(named.name());
            if (!(definition instanceof PipelineTemplateTypeDefinition.AliasType alias)) {
                return current;
            }
            current = alias.target();
        }
        return current;
    }

    public boolean isAssignable(String source, String target) {
        if (Objects.equals(source, target)) {
            return true;
        }
        PipelineTemplateTypeReference resolvedSource = resolveAliases(reference(source));
        PipelineTemplateTypeReference resolvedTarget = resolveAliases(reference(target));
        if (resolvedSource.equals(resolvedTarget)) {
            return true;
        }
        if (resolvedTarget instanceof PipelineTemplateTypeReference.Named namedTarget
            && definitions.get(namedTarget.name()) instanceof PipelineTemplateTypeDefinition.UnionType union) {
            return union.variants().values().stream()
                .map(PipelineTemplateTypeDefinition.Variant::payload)
                .map(this::resolveAliases)
                .anyMatch(resolvedSource::equals);
        }
        return false;
    }

    private static PipelineTemplateTypeReference reference(String value) {
        return PipelineTemplateTypeMappings.isV3ScalarType(value)
            ? new PipelineTemplateTypeReference.Scalar(value)
            : new PipelineTemplateTypeReference.Named(value);
    }

    private static void validate(Map<String, PipelineTemplateTypeDefinition> definitions) {
        for (Map.Entry<String, PipelineTemplateTypeDefinition> entry : definitions.entrySet()) {
            String name = entry.getKey();
            PipelineTemplateTypeDefinition definition = entry.getValue();
            if (name == null || name.isBlank() || definition == null || !name.equals(definition.name())) {
                throw new IllegalStateException("Invalid v3 type declaration");
            }
            if (PipelineTemplateTypeMappings.isBuiltinType(name)) {
                throw new IllegalStateException("Type name '" + name + "' conflicts with a built-in semantic type");
            }
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                Set<String> fields = new HashSet<>();
                for (PipelineTemplateTypeDefinition.Field field : record.fields()) {
                    if (field == null || field.name() == null || field.name().isBlank() || field.type() == null || !fields.add(field.name())) {
                        throw new IllegalStateException("Type '" + name + "' has duplicate or invalid field names");
                    }
                    validateReference(name + "." + field.name(), field.type(), definitions);
                }
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                if (wrapper.wraps() == null) {
                    throw new IllegalStateException("Type '" + name + "' wraps must reference a supported scalar");
                }
            } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
                validateReference(name + ".alias", alias.target(), definitions);
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                if (union.variants().isEmpty()) {
                    throw new IllegalStateException("Union '" + name + "' must declare at least one variant");
                }
                for (Map.Entry<String, PipelineTemplateTypeDefinition.Variant> entryVariant : union.variants().entrySet()) {
                    PipelineTemplateTypeDefinition.Variant variant = entryVariant.getValue();
                    if (entryVariant.getKey() == null || entryVariant.getKey().isBlank() || variant == null
                        || !entryVariant.getKey().equals(variant.discriminator())) {
                        throw new IllegalStateException("Union '" + name + "' has invalid discriminator declarations");
                    }
                    validateReference(name + "." + variant.discriminator(), variant.payload(), definitions);
                }
            }
        }
        for (String name : definitions.keySet()) {
            detectCycle(name, definitions, new ArrayList<>(), new HashSet<>());
        }
    }

    private static void validateReference(
        String owner,
        PipelineTemplateTypeReference reference,
        Map<String, PipelineTemplateTypeDefinition> definitions
    ) {
        if (reference instanceof PipelineTemplateTypeReference.Named named && !definitions.containsKey(named.name())) {
            throw new IllegalStateException("Type '" + owner + "' references unknown type '" + named.name() + "'");
        }
        if (reference instanceof PipelineTemplateTypeReference.MapType map) {
            validateReference(owner + " map value", map.valueType(), definitions);
        }
    }

    private static void detectCycle(
        String name,
        Map<String, PipelineTemplateTypeDefinition> definitions,
        List<String> path,
        Set<String> visited
    ) {
        if (path.contains(name)) {
            List<String> cycle = new ArrayList<>(path.subList(path.indexOf(name), path.size()));
            cycle.add(name);
            throw new IllegalStateException("Recursive v3 type reference is not supported: " + String.join(" -> ", cycle));
        }
        if (!visited.add(name)) {
            return;
        }
        path.add(name);
        for (String dependency : dependencies(definitions.get(name))) {
            detectCycle(dependency, definitions, path, visited);
        }
        path.removeLast();
    }

    private static List<String> dependencies(PipelineTemplateTypeDefinition definition) {
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
            return record.fields().stream().map(PipelineTemplateTypeDefinition.Field::type)
                .filter(PipelineTemplateTypeReference.Named.class::isInstance)
                .map(PipelineTemplateTypeReference.Named.class::cast).map(PipelineTemplateTypeReference.Named::name).toList();
        }
        if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias
            && alias.target() instanceof PipelineTemplateTypeReference.Named named) {
            return List.of(named.name());
        }
        if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
            return union.variants().values().stream().map(PipelineTemplateTypeDefinition.Variant::payload)
                .filter(PipelineTemplateTypeReference.Named.class::isInstance)
                .map(PipelineTemplateTypeReference.Named.class::cast)
                .map(PipelineTemplateTypeReference.Named::name).toList();
        }
        return List.of();
    }
}
