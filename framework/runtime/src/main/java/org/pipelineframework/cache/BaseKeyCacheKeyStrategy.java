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

package org.pipelineframework.cache;

import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import org.pipelineframework.context.PipelineContext;

/**
 * Fallback cache key strategy that uses the standard base key format.
 */
@ApplicationScoped
@Unremovable
public class BaseKeyCacheKeyStrategy implements CacheKeyStrategy {

    /**
     * Creates a new BaseKeyCacheKeyStrategy.
     */
    public BaseKeyCacheKeyStrategy() {
    }

    @Override
    public Optional<String> resolveKey(Object item, PipelineContext context) {
        if (item == null) {
            return Optional.empty();
        }
        String key = PipelineCacheKeyFormat.baseKey(item);
        if (key == null || key.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(key);
    }
}
