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
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.cache.PipelineCacheReader;

/**
 * Cache plugin adapter that exposes cache reads to the pipeline runtime.
 */
@ApplicationScoped
@Unremovable
public class PipelineCacheReaderAdapter implements PipelineCacheReader {

    @Inject
    CacheManager cacheManager;

    /**
     * Retrieve the cached value associated with the given key.
     *
     * @param key the cache key
     * @return an Optional containing the cached value if present, otherwise an empty Optional
     */
    @Override
    public Uni<Optional<Object>> get(String key) {
        return cacheManager.get(key);
    }

    /**
     * Check whether a cached value exists for the specified key.
     *
     * @param key the cache key to check for presence
     * @return `true` if a value is cached for the given key, `false` otherwise
     */
    @Override
    public Uni<Boolean> exists(String key) {
        return cacheManager.exists(key);
    }
}