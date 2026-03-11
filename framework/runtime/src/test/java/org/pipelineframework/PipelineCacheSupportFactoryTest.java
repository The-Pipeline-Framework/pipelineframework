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

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class PipelineCacheSupportFactoryTest {

    @Test
    void returnsNullWhenNoReadersPresent() {
        PipelineCacheSupportFactory factory = new PipelineCacheSupportFactory();
        factory.cacheReaders = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of());
        factory.cacheKeyStrategies = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of(new PipelineRunnerCacheReadTest.FixedKeyStrategy()));

        assertNull(factory.buildCacheReadSupport());
    }

    @Test
    void returnsNullWhenNoStrategiesPresent() {
        PipelineCacheSupportFactory factory = new PipelineCacheSupportFactory();
        factory.cacheReaders = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of(new PipelineRunnerCacheReadTest.HighPriorityReader()));
        factory.cacheKeyStrategies = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of());

        assertNull(factory.buildCacheReadSupport());
    }

    @Test
    void selectsHighestPriorityReaderAndPreservesPolicyWiring() {
        PipelineCacheSupportFactory factory = new PipelineCacheSupportFactory();
        factory.cacheReaders = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of(
            new PipelineRunnerCacheReadTest.LowPriorityReader(),
            new PipelineRunnerCacheReadTest.HighPriorityReader()));
        factory.cacheKeyStrategies = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of(new PipelineRunnerCacheReadTest.FixedKeyStrategy()));
        factory.cachePolicyDefault = "require-cache";

        PipelineRunner.CacheReadSupport support = factory.buildCacheReadSupport();

        assertNotNull(support);
        assertEquals("cached-high", support.reader.get("v1:key").await().indefinitely().orElseThrow());
        assertEquals(org.pipelineframework.cache.CachePolicy.REQUIRE_CACHE, support.resolvePolicy(null));
    }
}
