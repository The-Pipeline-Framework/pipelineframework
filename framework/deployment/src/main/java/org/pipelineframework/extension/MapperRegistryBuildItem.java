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

import io.quarkus.builder.item.SimpleBuildItem;
import io.smallrye.common.annotation.Experimental;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;

import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Build item containing the resolved mapper registry built from the Jandex index.
 * <p>
 * This build item is produced by the mapper inference build step and consumed
 * by code generation phases. It contains a complete mapping of domain types
 * to their corresponding Mapper implementations.
 * <p>
 * PER REQUIREMENTS: All mappers MUST be resolved at build time with zero runtime resolution.
 * "Fail fast on ambiguity" - "Never use runtime reflection" - "No fallback. No silent transport bypass."
 */
@Experimental("Mapper inference based on Jandex index")
public final class MapperRegistryBuildItem extends SimpleBuildItem {

    private final Map<DotName, ClassInfo> domainToMapper;
    private final Map<ClassInfo, DotName> mapperToDomain;

    /**
     * Creates a new mapper registry build item.
     *
     * @param domainToMapper map of domain type DotName to mapper ClassInfo
     * @param mapperToDomain map of mapper ClassInfo to domain type DotName
     */
    public MapperRegistryBuildItem(Map<DotName, ClassInfo> domainToMapper, Map<ClassInfo, DotName> mapperToDomain) {
        Map<DotName, ClassInfo> domainToMapperCopy = new HashMap<>(Objects.requireNonNull(domainToMapper));
        Map<ClassInfo, DotName> mapperToDomainCopy = new HashMap<>(Objects.requireNonNull(mapperToDomain));

        for (Map.Entry<DotName, ClassInfo> entry : domainToMapperCopy.entrySet()) {
            DotName domain = entry.getKey();
            ClassInfo mapper = entry.getValue();
            DotName reverseDomain = mapperToDomainCopy.get(mapper);
            if (!domain.equals(reverseDomain)) {
                throw new IllegalArgumentException(String.format(
                        "Inconsistent mapper registry: domainToMapper contains %s -> %s but mapperToDomain contains %s -> %s",
                        domain, mapper, mapper, reverseDomain));
            }
        }
        for (Map.Entry<ClassInfo, DotName> entry : mapperToDomainCopy.entrySet()) {
            ClassInfo mapper = entry.getKey();
            DotName domain = entry.getValue();
            ClassInfo reverseMapper = domainToMapperCopy.get(domain);
            if (!mapper.equals(reverseMapper)) {
                throw new IllegalArgumentException(String.format(
                        "Inconsistent mapper registry: mapperToDomain contains %s -> %s but domainToMapper contains %s -> %s",
                        mapper, domain, domain, reverseMapper));
            }
        }

        this.domainToMapper = Collections.unmodifiableMap(domainToMapperCopy);
        this.mapperToDomain = Collections.unmodifiableMap(mapperToDomainCopy);
    }

    /**
     * Gets the mapper ClassInfo for a given domain type.
     *
     * @param domainType the domain type DotName
     * @return the mapper ClassInfo, or null if no mapper is registered for this domain type
     */
    public ClassInfo getMapperForDomain(DotName domainType) {
        return domainToMapper.get(domainType);
    }

    /**
     * Gets the domain type for a given mapper.
     *
     * @param mapper the mapper ClassInfo
     * @return the domain type DotName, or null if the mapper is not in the registry
     */
    public DotName getDomainForMapper(ClassInfo mapper) {
        return mapperToDomain.get(mapper);
    }

    /**
     * Checks if a mapper is registered for a given domain type.
     *
     * @param domainType the domain type DotName
     * @return true if a mapper is registered, false otherwise
     */
    public boolean hasMapperForDomain(DotName domainType) {
        return domainToMapper.containsKey(domainType);
    }

    /**
     * Gets the number of registered mappers.
     *
     * @return the number of mappers in the registry
     */
    public int size() {
        return domainToMapper.size();
    }

    /**
     * Gets all registered domain types.
     *
     * @return an unmodifiable set of domain type DotNames
     */
    public Set<DotName> getAllDomainTypes() {
        return domainToMapper.keySet();
    }

    /**
     * Gets all registered mappers.
     *
     * @return an unmodifiable collection of mapper ClassInfos
     */
    public Collection<ClassInfo> getAllMappers() {
        return domainToMapper.values();
    }

    /**
     * Gets the underlying domain-to-mapper map.
     * <p>
     * Returns an unmodifiable view - modifications are not allowed.
     *
     * @return the domain-to-mapper map
     */
    public Map<DotName, ClassInfo> getDomainToMapperMap() {
        return domainToMapper;
    }

    @Override
    public String toString() {
        return "MapperRegistryBuildItem{size=" + domainToMapper.size() + '}';
    }
}
