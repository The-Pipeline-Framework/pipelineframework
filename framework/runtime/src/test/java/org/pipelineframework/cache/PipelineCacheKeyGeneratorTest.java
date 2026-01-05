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

class PipelineCacheKeyGeneratorTest {

    @Test
    void generate_PrefixesVersionTagWhenPresent() throws Exception {
        PipelineCacheKeyGenerator generator = new PipelineCacheKeyGenerator();
        Method method = Example.class.getDeclaredMethod("handle", TestKey.class);
        TestKey input = new TestKey("doc-123");

        PipelineContextHolder.set(new PipelineContext("v9", null, null));
        try {
            Object key = generator.generate(method, input);
            assertEquals("v9:" + TestKey.class.getName() + ":doc-123", key);
        } finally {
            PipelineContextHolder.clear();
        }
    }

    private static final class Example {
        @SuppressWarnings("unused")
        public void handle(TestKey key) {
        }
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
