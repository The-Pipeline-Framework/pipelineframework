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
    private final Map<String, Map<String, RepresentationMapping>> representationMappings;

    public PipelineTemplateTypeModel(Map<String, PipelineTemplateTypeDefinition> definitions) {
        this(definitions, Map.of());
    }

    public PipelineTemplateTypeModel(
        Map<String, PipelineTemplateTypeDefinition> definitions,
        Map<String, Map<String, RepresentationMapping>> representationMappings
    ) {
        Map<String, PipelineTemplateTypeDefinition> copy = definitions == null
            ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(definitions));
        validate(copy);
        this.definitions = copy;
        this.representationMappings = normalizeRepresentationMappings(copy, representationMappings);
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

    /**
     * All external representation declarations, grouped by named v3 domain type then component key.
     */
    public Map<String, Map<String, RepresentationMapping>> representationMappings() {
        return representationMappings;
    }

    /** Finds an optional representation declaration without imposing consumer policy. */
    public Optional<RepresentationMapping> representationMapping(String domainType, String key) {
        return Optional.ofNullable(representationMappings.getOrDefault(domainType, Map.of()).get(key));
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

    private static Map<String, Map<String, RepresentationMapping>> normalizeRepresentationMappings(
        Map<String, PipelineTemplateTypeDefinition> definitions,
        Map<String, Map<String, RepresentationMapping>> mappings
    ) {
        if (mappings == null || mappings.isEmpty()) {
            return Map.of();
        }
        Map<String, Map<String, RepresentationMapping>> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, RepresentationMapping>> typeEntry : mappings.entrySet()) {
            String domainType = typeEntry.getKey();
            if (!definitions.containsKey(domainType)) {
                throw new IllegalStateException("Representation mapping references unknown type '" + domainType + "'");
            }
            Map<String, RepresentationMapping> byKey = new LinkedHashMap<>();
            Map<String, RepresentationMapping> declared = typeEntry.getValue() == null ? Map.of() : typeEntry.getValue();
            for (Map.Entry<String, RepresentationMapping> mappingEntry : declared.entrySet()) {
                RepresentationMapping mapping = mappingEntry.getValue();
                if (mappingEntry.getKey() == null || mappingEntry.getKey().isBlank() || mapping == null
                    || !mappingEntry.getKey().equals(mapping.key()) || !domainType.equals(mapping.domainType())) {
                    throw new IllegalStateException("Invalid representation mapping for type '" + domainType + "'");
                }
                if (byKey.putIfAbsent(mapping.key(), mapping) != null) {
                    throw new IllegalStateException("Type '" + domainType + "' declares duplicate representation mapping '" + mapping.key() + "'");
                }
            }
            if (!byKey.isEmpty()) {
                normalized.put(domainType, Collections.unmodifiableMap(byKey));
            }
        }
        return Collections.unmodifiableMap(normalized);
    }

    private static void validateReference(
        String owner,
        PipelineTemplateTypeReference reference,
        Map<String, PipelineTemplateTypeDefinition> definitions
    ) {
        if (reference == null) {
            throw new IllegalStateException("Type '" + owner + "' has an invalid type reference");
        }
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
        List<String> dependencies = new ArrayList<>();
        if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
            record.fields().forEach(field -> collectNamedReferences(field.type(), dependencies));
        } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
            collectNamedReferences(alias.target(), dependencies);
        } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
            union.variants().values().forEach(variant -> collectNamedReferences(variant.payload(), dependencies));
        }
        return List.copyOf(dependencies);
    }

    private static void collectNamedReferences(PipelineTemplateTypeReference reference, List<String> dependencies) {
        if (reference instanceof PipelineTemplateTypeReference.Named named) {
            dependencies.add(named.name());
        } else if (reference instanceof PipelineTemplateTypeReference.MapType map) {
            collectNamedReferences(map.valueType(), dependencies);
        }
    }
}
