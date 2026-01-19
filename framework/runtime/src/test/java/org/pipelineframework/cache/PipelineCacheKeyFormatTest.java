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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineCacheKeyFormatTest {

    @Test
    void baseKey_UsesTypeAndCacheKey() {
        TestKey key = new TestKey("doc-1");
        String baseKey = PipelineCacheKeyFormat.baseKey(key);

        assertEquals(TestKey.class.getName() + ":doc-1", baseKey);
    }

    @Test
    void baseKeyForParams_JoinsParamsWithTypes() {
        TestKey key1 = new TestKey("doc-1");
        TestKey key2 = new TestKey("doc-2");
        String combined = PipelineCacheKeyFormat.baseKeyForParams(key1, key2, "extra");

        assertTrue(combined.contains(TestKey.class.getName() + ":doc-1"));
        assertTrue(combined.contains(TestKey.class.getName() + ":doc-2"));
        assertTrue(combined.contains(String.class.getName() + ":extra"));
    }

    @Test
    void applyVersionTag_PrefixesWhenPresent() {
        String tagged = PipelineCacheKeyFormat.applyVersionTag("key", "v1");
        assertEquals("v1:key", tagged);
    }

    @Test
    void typePrefix_IncludesTypeAndVersion() {
        String prefix = PipelineCacheKeyFormat.typePrefix(TestKey.class, "v2");
        assertEquals("v2:" + TestKey.class.getName() + ":", prefix);
    }

    private static final class TestKey implements CacheKey {
        private final String id;

        private TestKey(String id) {
            this.id = id;
        }

        @Override
        public String cacheKey() {
            return id;
        }
    }
}
