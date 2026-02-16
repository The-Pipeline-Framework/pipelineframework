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

package org.pipelineframework.extension;

import io.smallrye.common.annotation.Experimental;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;
import org.jboss.jandex.WildcardType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Build-time mapper inference engine using Jandex index for full classpath visibility.
 * <p>
 * This engine discovers Mapper implementations from the complete application classpath
 * via the Jandex index provided by Quarkus' CombinedIndexBuildItem. It performs strict
 * generic type validation and fails fast on ambiguity.
 * <p>
 * PER REQUIREMENTS:
 * <ul>
 *   <li>All mappers MUST be resolved at build time - zero runtime resolution</li>
 *   <li>Fail fast on ambiguity</li>
 *   <li>Never use runtime reflection</li>
 *   <li>No fallback. No silent transport bypass.</li>
 * </ul>
 * <p>
 * Mapper inference algorithm:
 * <ol>
 *   <li>Scan all known implementors of {@code org.pipelineframework.mapper.Mapper} via Jandex</li>
 *   <li>Extract generic type parameters from Mapper&lt;Grpc, Dto, Domain&gt;</li>
 *   <li>Validate: no wildcards, no raw types, no erased generics</li>
 *   <li>Build registry: Domain type → Mapper implementation</li>
 *   <li>Validate uniqueness: exactly one mapper per domain type</li>
 * </ol>
 */
@Experimental("Mapper inference based on Jandex index")
public class MapperInferenceEngine {

    private static final DotName MAPPER_INTERFACE = DotName.createSimple("org.pipelineframework.mapper.Mapper");
    private final IndexView index;

    /**
     * Creates a new mapper inference engine.
     *
     * @param index the Jandex index containing the full application classpath
     */
    public MapperInferenceEngine(IndexView index) {
        this.index = Objects.requireNonNull(index, "Jandex index must not be null");
    }

    /**
     * Result of mapper inference containing the resolved mapper or error information.
     * <p>
     * This record enforces invariants via compact constructor:
     * <ul>
     *   <li>When success=true: mapperClass must be non-null, errorMessage must be null</li>
     *   <li>When success=false: mapperClass must be null, errorMessage must be non-null</li>
     * </ul>
     *
     * @param mapperClass the resolved mapper ClassInfo, or null if resolution failed
     * @param success whether resolution was successful
     * @param errorMessage error message if resolution failed, or null if successful
     */
    public record InferenceResult(ClassInfo mapperClass, boolean success, String errorMessage) {
        /**
         * Compact constructor enforcing invariants.
         */
        public InferenceResult {
            if (success) {
                if (mapperClass == null) {
                    throw new IllegalArgumentException(
                        "InferenceResult: success=true requires non-null mapperClass. " +
                        "PER REQUIREMENTS: All mappers MUST be resolved at build time. No runtime resolution allowed.");
                }
                if (errorMessage != null) {
                    throw new IllegalArgumentException(
                        "InferenceResult: success=true requires null errorMessage");
                }
            } else {
                if (mapperClass != null) {
                    throw new IllegalArgumentException(
                        "InferenceResult: success=false requires mapperClass==null");
                }
                if (errorMessage == null || errorMessage.isBlank()) {
                    throw new IllegalArgumentException(
                        "InferenceResult: success=false requires non-null, non-blank errorMessage");
                }
            }
        }
    }

    /**
     * Builds a complete mapper registry from the Jandex index.
     * <p>
     * This method scans all known implementors of the Mapper interface,
     * extracts their generic type parameters, validates them, and builds
     * a registry mapping domain types to mapper implementations.
     *
     * @return the built mapper registry
     * @throws IllegalStateException if duplicate mappers are found for the same domain type
     */
    public MapperRegistry buildRegistry() {
        // Get all classes implementing Mapper interface
        Collection<ClassInfo> mapperImplementors = index.getAllKnownImplementors(MAPPER_INTERFACE);

        if (mapperImplementors.isEmpty()) {
            // No mappers found - this may be valid for some applications
            return new MapperRegistry(Map.of(), Map.of());
        }

        Map<DotName, ClassInfo> domainToMapper = new HashMap<>();
        Map<ClassInfo, DotName> mapperToDomain = new HashMap<>();
        List<String> validationErrors = new ArrayList<>();

        for (ClassInfo mapper : mapperImplementors) {
            // Extract generic signature from Mapper<Grpc, Dto, Domain>
            MapperGenericSignature signature = extractMapperGenericSignature(mapper);
            if (signature == null) {
                validationErrors.add("Mapper " + mapper.name() + " has invalid/erased generic parameters");
                continue;
            }

            // Validate no wildcards or erased types
            if (signature.hasWildcardOrErased()) {
                validationErrors.add("Mapper " + mapper.name() + " contains wildcards or erased types: " + signature);
                continue;
            }

            DotName domainType = signature.domainType().name();

            // Check for duplicates - FAIL FAST on ambiguity
            if (domainToMapper.containsKey(domainType)) {
                ClassInfo existingMapper = domainToMapper.get(domainType);
                validationErrors.add(String.format(
                    "Duplicate mapper found for domain type '%s': %s and %s. " +
                    "PER REQUIREMENTS: Exactly one mapper per domain type required.",
                    domainType, existingMapper.name(), mapper.name()));
            } else {
                domainToMapper.put(domainType, mapper);
                mapperToDomain.put(mapper, domainType);
            }
        }

        // Fail fast if any validation errors occurred
        if (!validationErrors.isEmpty()) {
            throw new IllegalStateException("Mapper validation failed:\n" +
                String.join("\n", validationErrors));
        }

        return new MapperRegistry(domainToMapper, mapperToDomain);
    }

    /**
     * Extracts the generic type signature from a Mapper implementation.
     * <p>
     * Looks for the Mapper interface in the class's type parameters and
     * extracts the three generic arguments: {@code Mapper<Grpc, Dto, Domain>}.
     *
     * @param mapperClass the mapper ClassInfo
     * @return the extracted generic signature, or null if extraction failed
     */
    private MapperGenericSignature extractMapperGenericSignature(ClassInfo mapperClass) {
        // Check interfaces first, including extended interfaces.
        for (Type interfaceType : mapperClass.interfaceTypes()) {
            MapperGenericSignature signature = resolveMapperSignature(interfaceType, new HashSet<>());
            if (signature != null) {
                return signature;
            }
        }

        // Check superclass chain, including inherited interfaces on abstract bases.
        Type superClassType = mapperClass.superClassType();
        if (superClassType != null) {
            return resolveMapperSignature(superClassType, new HashSet<>());
        }

        return null;
    }

    private MapperGenericSignature resolveMapperSignature(Type type, Set<DotName> visited) {
        if (type == null) {
            return null;
        }
        DotName typeName = type.name();
        if (typeName != null && !visited.add(typeName)) {
            return null;
        }

        if (isMapperInterface(type)) {
            if (type.kind() == Type.Kind.PARAMETERIZED_TYPE) {
                ParameterizedType parameterizedType = type.asParameterizedType();
                List<Type> typeArguments = parameterizedType.arguments();
                if (typeArguments.size() == 3) {
                    return new MapperGenericSignature(
                        typeArguments.get(0),
                        typeArguments.get(1),
                        typeArguments.get(2));
                }
            }
            // Mapper found but not parameterized.
            return null;
        }

        ClassInfo classInfo = typeName != null ? index.getClassByName(typeName) : null;
        if (classInfo == null) {
            return null;
        }

        for (Type interfaceType : classInfo.interfaceTypes()) {
            MapperGenericSignature signature = resolveMapperSignature(interfaceType, visited);
            if (signature != null) {
                return signature;
            }
        }

        return resolveMapperSignature(classInfo.superClassType(), visited);
    }

    /**
     * Checks if a Type represents the Mapper interface.
     *
     * @param type the type to check
     * @return true if the type is the Mapper interface, false otherwise
     */
    private boolean isMapperInterface(Type type) {
        if (type == null) {
            return false;
        }

        DotName typeName = type.name();
        return MAPPER_INTERFACE.equals(typeName);
    }

    /**
     * Holds the extracted generic type parameters from a Mapper implementation.
     *
     * @param grpcType the first generic parameter (Grpc message type)
     * @param dtoType the second generic parameter (DTO type)
     * @param domainType the third generic parameter (Domain entity type)
     */
    private record MapperGenericSignature(Type grpcType, Type dtoType, Type domainType) {
        /**
         * Checks if any of the type parameters are wildcards or erased.
         *
         * @return true if wildcards or erased types are present
         */
        boolean hasWildcardOrErased() {
            return isWildcardOrErased(grpcType) ||
                   isWildcardOrErased(dtoType) ||
                   isWildcardOrErased(domainType);
        }

        /**
         * Checks if a type is a wildcard or has erased generic parameters.
         *
         * @param type the type to check
         * @return true if the type is a wildcard or erased
         */
        private boolean isWildcardOrErased(Type type) {
            if (type == null) {
                return true;
            }

            // Check for wildcard types
            if (type.kind() == Type.Kind.WILDCARD_TYPE) {
                return true;
            }

            // Unresolved generic type variables are considered erased for inference.
            if (type.kind() == Type.Kind.TYPE_VARIABLE) {
                return true;
            }

            return false;
        }

        @Override
        public String toString() {
            return "Mapper<" + grpcType + ", " + dtoType + ", " + domainType + ">";
        }
    }

    /**
     * Immutable mapper registry built from Jandex index.
     */
    public record MapperRegistry(
        Map<DotName, ClassInfo> domainToMapper,
        Map<ClassInfo, DotName> mapperToDomain
    ) {
        /**
         * Creates a new mapper registry.
         */
        public MapperRegistry {
            domainToMapper = Map.copyOf(domainToMapper);
            mapperToDomain = Map.copyOf(mapperToDomain);
        }
    }
}
