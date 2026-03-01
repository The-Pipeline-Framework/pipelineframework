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

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.smallrye.common.annotation.Experimental;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.IndexView;
import org.jboss.logging.Logger;
import org.pipelineframework.extension.MapperInferenceEngine.MapperRegistry;

import java.util.Map;
import java.util.List;

/**
 * Build steps for mapper inference using the Jandex index.
 * <p>
 * This class provides the build step that consumes the CombinedIndexBuildItem
 * and produces the MapperRegistryBuildItem containing all resolved mapper pair assignments.
 * <p>
 * PER REQUIREMENTS:
 * <ul>
 *   <li>All mappers MUST be resolved at build time - zero runtime resolution</li>
 *   <li>Use Jandex index for full classpath visibility</li>
 *   <li>Fail fast on ambiguity</li>
 *   <li>No fallback. No silent transport bypass.</li>
 * </ul>
 */
@Experimental("Mapper inference based on Jandex index")
public class MapperInferenceBuildSteps {

    private static final Logger LOG = Logger.getLogger(MapperInferenceBuildSteps.class);

    /**
     * Builds and emits a MapperRegistryBuildItem representing mappers discovered in the application's Jandex index.
     *
     * Discovers Mapper implementations in the index and produces a MapperRegistryBuildItem for downstream build steps.
     * Step-specific mapper validation is demand-driven and performed by consumers that know required inbound/outbound pairs.
     *
     * @param combinedIndex the combined Jandex index of application and dependency classes
     * @param operators optional list of OperatorBuildItem used to derive step definitions; when empty, step definitions are read from resources
     * @param mapperRegistry producer used to emit the resulting MapperRegistryBuildItem
     */
    @BuildStep
    void buildMapperRegistry(
            CombinedIndexBuildItem combinedIndex,
            List<OperatorBuildItem> operators,
            BuildProducer<MapperRegistryBuildItem> mapperRegistry) {

        LOG.debugf("Building mapper registry from Jandex index");

        IndexView index = combinedIndex.getIndex();
        MapperInferenceEngine engine = new MapperInferenceEngine(index);

        // Build the mapper registry from all discovered mappers
        MapperRegistry registry = engine.buildRegistry();

        LOG.debugf("Mapper registry built with %d mapper pairs", registry.pairToMapper().size());

        // Log discovered mappers for debugging
        if (LOG.isDebugEnabled()) {
            for (Map.Entry<MapperInferenceEngine.MapperPairKey, ClassInfo> entry : registry.pairToMapper().entrySet()) {
                LOG.debugf("  Mapper for (%s, %s): %s",
                        entry.getKey().domainType(),
                        entry.getKey().externalType(),
                        entry.getValue().name());
            }
        }

        // Produce the registry build item
        mapperRegistry.produce(new MapperRegistryBuildItem(
                registry.pairToMapper(),
                registry.mapperToPair()));
    }

    /**
     * Delegates to the main buildMapperRegistry overload using an empty operator list; provided for unit tests.
     *
     * @param combinedIndex the combined Jandex index build item used to locate types
     * @param mapperRegistry producer to emit the resulting MapperRegistryBuildItem
     */
    void buildMapperRegistry(
            CombinedIndexBuildItem combinedIndex,
            BuildProducer<MapperRegistryBuildItem> mapperRegistry) {
        buildMapperRegistry(combinedIndex, List.of(), mapperRegistry);
    }

}
