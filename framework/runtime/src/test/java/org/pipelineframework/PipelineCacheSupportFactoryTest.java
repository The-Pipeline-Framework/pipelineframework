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

    @Test
    void tiesCacheKeyStrategiesByClassNameDeterministically() {
        PipelineCacheSupportFactory factory = new PipelineCacheSupportFactory();
        factory.cacheReaders = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of(new PipelineRunnerCacheReadTest.HighPriorityReader()));
        factory.cacheKeyStrategies = new PipelineRunnerCacheReadTest.SimpleInstance<>(List.of(
            new ZStrategy(),
            new AStrategy()));
        factory.cachePolicyDefault = "prefer-cache";

        PipelineRunner.CacheReadSupport support = factory.buildCacheReadSupport();

        assertNotNull(support);
        assertEquals("a", support.resolveKey("input", null).orElseThrow());
    }

    static final class AStrategy implements org.pipelineframework.cache.CacheKeyStrategy {
        @Override
        public java.util.Optional<String> resolveKey(Object item, org.pipelineframework.context.PipelineContext context) {
            return java.util.Optional.of("a");
        }

        @Override
        public int priority() {
            return 100;
        }
    }

    static final class ZStrategy implements org.pipelineframework.cache.CacheKeyStrategy {
        @Override
        public java.util.Optional<String> resolveKey(Object item, org.pipelineframework.context.PipelineContext context) {
            return java.util.Optional.of("z");
        }

        @Override
        public int priority() {
            return 100;
        }
    }
}
