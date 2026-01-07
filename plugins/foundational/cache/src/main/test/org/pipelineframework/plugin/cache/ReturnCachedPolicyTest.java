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

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.context.PipelineCacheStatusHolder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class ReturnCachedPolicyTest {

    @AfterEach
    void cleanup() {
        PipelineCacheStatusHolder.clear();
    }

    @Test
    void handle_ReturnsCachedOnHit() {
        CacheManager cacheManager = mock(CacheManager.class);
        ReturnCachedPolicy policy = new ReturnCachedPolicy(cacheManager, Logger.getLogger(ReturnCachedPolicy.class));

        TestItem item = new TestItem("key-1");
        TestItem cached = new TestItem("key-1");
        when(cacheManager.get("v1:key-1")).thenReturn(Uni.createFrom().item(Optional.of(cached)));

        Uni<TestItem> result = policy.handle(item, item.cacheKey(), key -> "v1:" + key);
        UniAssertSubscriber<TestItem> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(cached, subscriber.getItem());
        assertEquals(CacheStatus.HIT, PipelineCacheStatusHolder.getAndClear());
        verify(cacheManager).get("v1:key-1");
        verify(cacheManager, never()).cache(any());
    }

    @Test
    void handle_CachesOnMiss() {
        CacheManager cacheManager = mock(CacheManager.class);
        ReturnCachedPolicy policy = new ReturnCachedPolicy(cacheManager, Logger.getLogger(ReturnCachedPolicy.class));

        TestItem item = new TestItem("key-2");
        when(cacheManager.get("key-2")).thenReturn(Uni.createFrom().item(Optional.empty()));
        when(cacheManager.cache(item)).thenReturn(Uni.createFrom().item(item));

        Uni<TestItem> result = policy.handle(item, item.cacheKey(), key -> key);
        UniAssertSubscriber<TestItem> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        assertEquals(CacheStatus.MISS, PipelineCacheStatusHolder.getAndClear());
        verify(cacheManager).cache(item);
    }

    private static final class TestItem implements org.pipelineframework.cache.CacheKey {
        private final String id;

        private TestItem(String id) {
            this.id = id;
        }

        @Override
        public String cacheKey() {
            return id;
        }
    }
}
