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

package org.pipelineframework;

import java.util.Comparator;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.cache.PipelineCacheReader;

@ApplicationScoped
class PipelineCacheSupportFactory {

    private static final Logger logger = Logger.getLogger(PipelineCacheSupportFactory.class);

    @Inject
    Instance<CacheKeyStrategy> cacheKeyStrategies;

    @Inject
    Instance<PipelineCacheReader> cacheReaders;

    @ConfigProperty(name = "pipeline.cache.policy", defaultValue = "prefer-cache")
    String cachePolicyDefault;

    PipelineRunner.CacheReadSupport buildCacheReadSupport() {
        if (cacheReaders == null) {
            return null;
        }
        List<PipelineCacheReader> readers = cacheReaders.stream()
            .sorted(Comparator.comparingInt(PipelineCacheReader::priority).reversed()
                .thenComparing(cacheReader -> cacheReader.getClass().getName()))
            .toList();
        if (readers.isEmpty() || cacheKeyStrategies == null) {
            return null;
        }
        if (readers.size() > 1) {
            String readerNames = String.join(
                ", ",
                readers.stream()
                    .map(cacheReader -> cacheReader.getClass().getName() + "(priority=" + cacheReader.priority() + ")")
                    .toList());
            logger.warnf(
                "Multiple PipelineCacheReader beans found (%s). Using %s based on priority ordering.",
                readerNames,
                readers.get(0).getClass().getName());
        }
        PipelineCacheReader reader = readers.get(0);
        List<CacheKeyStrategy> ordered = cacheKeyStrategies.stream()
            .sorted(Comparator.comparingInt(CacheKeyStrategy::priority).reversed()
                .thenComparing(strategy -> strategy.getClass().getName()))
            .toList();
        if (ordered.isEmpty()) {
            return null;
        }
        return new PipelineRunner.CacheReadSupport(reader, ordered, cachePolicyDefault);
    }
}
