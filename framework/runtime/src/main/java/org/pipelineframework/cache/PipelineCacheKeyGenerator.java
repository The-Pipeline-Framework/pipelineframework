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

import java.lang.reflect.Method;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

/**
 * Cache key generator that prefixes keys with the current pipeline version tag.
 */
@ApplicationScoped
@Unremovable
public class PipelineCacheKeyGenerator implements CacheKeyGenerator {

    /**
     * Creates a new PipelineCacheKeyGenerator.
     */
    public PipelineCacheKeyGenerator() {
    }

    @Override
    public Object generate(Method method, Object... methodParams) {
        String baseKey = PipelineCacheKeyFormat.baseKeyForParams(methodParams);

        PipelineContext context = PipelineContextHolder.get();
        String versionTag = context != null ? context.versionTag() : null;
        return PipelineCacheKeyFormat.applyVersionTag(baseKey, versionTag);
    }
}
