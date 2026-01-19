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

import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertyCacheKeyGeneratorTest {

    @Test
    void docIdGenerator_UsesDocIdAndVersionTag() throws Exception {
        DocIdCacheKeyGenerator generator = new DocIdCacheKeyGenerator();
        Method method = Example.class.getDeclaredMethod("handleDoc", DocItem.class);
        DocItem input = new DocItem("doc-9");

        PipelineContextHolder.set(new PipelineContext("v3", null, null));
        try {
            Object key = generator.generate(method, input);
            assertEquals("v3:" + DocItem.class.getName() + ":doc-9", key);
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void idGenerator_UsesIdGetter() throws Exception {
        IdCacheKeyGenerator generator = new IdCacheKeyGenerator();
        Method method = Example.class.getDeclaredMethod("handleId", IdItem.class);
        IdItem input = new IdItem("id-7");

        Object key = generator.generate(method, input);
        assertEquals(IdItem.class.getName() + ":id-7", key);
    }

    @Test
    void generator_FallsBackToCacheKeyWhenPropertyMissing() throws Exception {
        DocIdCacheKeyGenerator generator = new DocIdCacheKeyGenerator();
        Method method = Example.class.getDeclaredMethod("handleKey", CacheKeyOnly.class);
        CacheKeyOnly input = new CacheKeyOnly("cache-1");

        Object key = generator.generate(method, input);
        assertEquals(CacheKeyOnly.class.getName() + ":cache-1", key);
    }

    private static final class Example {
        @SuppressWarnings("unused")
        public void handleDoc(DocItem input) {
        }

        @SuppressWarnings("unused")
        public void handleId(IdItem input) {
        }

        @SuppressWarnings("unused")
        public void handleKey(CacheKeyOnly input) {
        }
    }

    private record DocItem(String docId) {
    }

    private static final class IdItem {
        private final String id;

        private IdItem(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    private static final class CacheKeyOnly implements CacheKey {
        private final String key;

        private CacheKeyOnly(String key) {
            this.key = key;
        }

        @Override
        public String cacheKey() {
            return key;
        }
    }
}
