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
import org.pipelineframework.extension.MapperInferenceEngine.MapperPairKey;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Build item containing the resolved pair-based mapper registry built from the Jandex index.
 * <p>
 * Mappers are keyed by exact (domain, external) type pairs so inbound and outbound conversions
 * can be resolved independently without domain-only ambiguity.
 */
@Experimental("Mapper inference based on Jandex index")
public final class MapperRegistryBuildItem extends SimpleBuildItem {

    private final Map<MapperPairKey, ClassInfo> pairToMapper;
    private final Map<ClassInfo, MapperPairKey> mapperToPair;

    /**
     * Constructs a build item that holds a bidirectional immutable mapping between mapper pair keys and mapper classes.
     *
     * @param pairToMapper map from (domain, external) pair key to mapper implementation
     * @param mapperToPair map from mapper implementation to (domain, external) pair key
     */
    public MapperRegistryBuildItem(Map<MapperPairKey, ClassInfo> pairToMapper, Map<ClassInfo, MapperPairKey> mapperToPair) {
        Map<MapperPairKey, ClassInfo> pairToMapperCopy = new HashMap<>(Objects.requireNonNull(pairToMapper));
        Map<ClassInfo, MapperPairKey> mapperToPairCopy = new HashMap<>(Objects.requireNonNull(mapperToPair));

        if (pairToMapperCopy.size() != mapperToPairCopy.size()) {
            throw new IllegalArgumentException(String.format(
                    "Inconsistent mapper registry: pairToMapper has %d entries but mapperToPair has %d entries.",
                    pairToMapperCopy.size(),
                    mapperToPairCopy.size()));
        }

        for (Map.Entry<MapperPairKey, ClassInfo> entry : pairToMapperCopy.entrySet()) {
            MapperPairKey pair = entry.getKey();
            ClassInfo mapper = entry.getValue();
            MapperPairKey reversePair = mapperToPairCopy.get(mapper);
            if (!pair.equals(reversePair)) {
                throw new IllegalArgumentException(String.format(
                        "Inconsistent mapper registry: pairToMapper contains %s -> %s but mapperToPair contains %s -> %s",
                        pair, mapper, mapper, reversePair));
            }
        }
        for (Map.Entry<ClassInfo, MapperPairKey> entry : mapperToPairCopy.entrySet()) {
            ClassInfo mapper = entry.getKey();
            MapperPairKey pair = entry.getValue();
            ClassInfo reverseMapper = pairToMapperCopy.get(pair);
            if (!mapper.equals(reverseMapper)) {
                throw new IllegalArgumentException(String.format(
                        "Inconsistent mapper registry: mapperToPair contains %s -> %s but pairToMapper contains %s -> %s",
                        mapper, pair, pair, reverseMapper));
            }
        }

        this.pairToMapper = Collections.unmodifiableMap(pairToMapperCopy);
        this.mapperToPair = Collections.unmodifiableMap(mapperToPairCopy);
    }

    /**
     * Gets the mapper for an exact (domain, external) pair.
     *
     * @param domainType domain/internal type
     * @param externalType external representation type
     * @return mapper class or null if no mapper is registered for the pair
     */
    public ClassInfo getMapperForPair(DotName domainType, DotName externalType) {
        return pairToMapper.get(new MapperPairKey(domainType, externalType));
    }

    /**
     * Checks if a mapper is registered for an exact (domain, external) pair.
     *
     * @param domainType domain/internal type
     * @param externalType external representation type
     * @return true when a mapper exists for the pair
     */
    public boolean hasMapperForPair(DotName domainType, DotName externalType) {
        return pairToMapper.containsKey(new MapperPairKey(domainType, externalType));
    }

    /**
     * Gets the pair key for a given mapper class.
     *
     * @param mapper mapper implementation class
     * @return pair key or null if mapper is not in registry
     */
    public MapperPairKey getPairForMapper(ClassInfo mapper) {
        return mapperToPair.get(mapper);
    }

    /**
     * Number of registered mapper pairs.
     *
     * @return number of pair entries in the registry
     */
    public int size() {
        return pairToMapper.size();
    }

    /**
     * All registered pair keys.
     *
     * @return unmodifiable set of pair keys
     */
    public Set<MapperPairKey> getAllPairs() {
        return pairToMapper.keySet();
    }

    /**
     * All registered mapper classes.
     *
     * @return unmodifiable collection of mapper classes
     */
    public Collection<ClassInfo> getAllMappers() {
        return pairToMapper.values();
    }

    /**
     * Underlying pair-to-mapper map.
     *
     * @return unmodifiable map from pair key to mapper class
     */
    public Map<MapperPairKey, ClassInfo> getPairToMapperMap() {
        return pairToMapper;
    }

    /**
     * Underlying mapper-to-pair map.
     *
     * @return unmodifiable map from mapper class to pair key
     */
    public Map<ClassInfo, MapperPairKey> getMapperToPairMap() {
        return mapperToPair;
    }

    @Override
    public String toString() {
        return "MapperRegistryBuildItem{size=" + pairToMapper.size() + '}';
    }
}
