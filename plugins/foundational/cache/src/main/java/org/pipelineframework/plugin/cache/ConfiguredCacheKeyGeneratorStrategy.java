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

package org.pipelineframework.plugin.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;

@ApplicationScoped
@Unremovable
public class ConfiguredCacheKeyGeneratorStrategy implements CacheKeyStrategy {

    private static final Logger LOG = Logger.getLogger(ConfiguredCacheKeyGeneratorStrategy.class);

    @ConfigProperty(name = "pipeline.cache.keyGenerator")
    Optional<String> generatorClassName;

    @Inject
    Instance<CacheKeyGenerator> generators;

    @Override
    public Optional<String> resolveKey(Object item, PipelineContext context) {
        if (item == null || generatorClassName.isEmpty()) {
            return Optional.empty();
        }
        CacheKeyGenerator generator = resolveGenerator(generatorClassName.get());
        if (generator == null) {
            return Optional.empty();
        }
        Object key = generator.generate(null, item);
        if (key == null) {
            return Optional.empty();
        }
        String normalized = key.toString();
        if (normalized.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(normalized);
    }

    @Override
    public int priority() {
        return 100;
    }

    private CacheKeyGenerator resolveGenerator(String className) {
        if (className == null || className.isBlank()) {
            return null;
        }
        try {
            Class<?> candidate = Class.forName(className.trim());
            if (!CacheKeyGenerator.class.isAssignableFrom(candidate)) {
                LOG.warnf("Configured cache key generator %s does not implement CacheKeyGenerator", className);
                return null;
            }
            @SuppressWarnings("unchecked")
            Class<? extends CacheKeyGenerator> generatorClass =
                (Class<? extends CacheKeyGenerator>) candidate;
            Instance<? extends CacheKeyGenerator> selected = generators.select(generatorClass);
            if (selected.isUnsatisfied()) {
                LOG.warnf("No CacheKeyGenerator bean found for %s", className);
                return null;
            }
            return selected.get();
        } catch (ClassNotFoundException e) {
            LOG.warnf("Cache key generator class not found: %s", className);
            return null;
        }
    }
}
