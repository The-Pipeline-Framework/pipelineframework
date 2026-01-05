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

import java.lang.reflect.Field;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheKey;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class CacheServiceTest {

    @Test
    void process_WithNullItem_ShouldReturnNull() {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);

        Uni<TestItem> resultUni = service.process((TestItem) null);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void process_WithNonCacheKey_ShouldReturnSameItem() {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<Object> service = new CacheService<>(cacheManager);

        Object item = new Object();
        Uni<Object> resultUni = service.process(item);
        UniAssertSubscriber<Object> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verifyNoInteractions(cacheManager);
    }

    @Test
    void process_WithCacheOnlyPolicy_ShouldCache() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "cache-only");

        TestItem item = new TestItem("id-1");
        when(cacheManager.cache(item)).thenReturn(Uni.createFrom().item(item));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager).cache(item);
    }

    @Test
    void process_WithReturnCachedPolicy_ShouldReturnCachedWhenPresent() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "return-cached");

        TestItem item = new TestItem("id-2");
        TestItem cached = new TestItem("id-2");
        when(cacheManager.get(item.cacheKey())).thenReturn(Uni.createFrom().item(Optional.of(cached)));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(cached, subscriber.getItem());
        verify(cacheManager, never()).cache(any());
    }

    @Test
    void process_WithReturnCachedPolicy_ShouldCacheOnMiss() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "return-cached");

        TestItem item = new TestItem("id-3");
        when(cacheManager.get(item.cacheKey())).thenReturn(Uni.createFrom().item(Optional.empty()));
        when(cacheManager.cache(item)).thenReturn(Uni.createFrom().item(item));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager).cache(item);
    }

    @Test
    void process_WithSkipIfPresentPolicy_ShouldSkipWhenExists() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "skip-if-present");

        TestItem item = new TestItem("id-4");
        when(cacheManager.exists(item.cacheKey())).thenReturn(Uni.createFrom().item(true));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager, never()).cache(any());
    }

    @Test
    void process_WithSkipIfPresentPolicy_ShouldCacheWhenMissing() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "skip-if-present");

        TestItem item = new TestItem("id-5");
        when(cacheManager.exists(item.cacheKey())).thenReturn(Uni.createFrom().item(false));
        when(cacheManager.cache(item)).thenReturn(Uni.createFrom().item(item));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager).cache(item);
    }

    @Test
    void process_WithContextPolicyOverride_ShouldUseOverride() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "cache-only");

        TestItem item = new TestItem("id-6");
        PipelineContextHolder.set(new PipelineContext(null, null, "return-cached"));
        when(cacheManager.get(item.cacheKey())).thenReturn(Uni.createFrom().item(Optional.of(item)));

        try {
            Uni<TestItem> resultUni = service.process(item);
            UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
            subscriber.awaitItem();

            assertSame(item, subscriber.getItem());
            verify(cacheManager).get(item.cacheKey());
            verify(cacheManager, never()).cache(any());
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void process_WithVersionTag_ShouldPrefixCacheKey() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager);
        setPolicy(service, "return-cached");

        TestItem item = new TestItem("id-7");
        PipelineContextHolder.set(new PipelineContext("v1", null, null));
        when(cacheManager.get("v1:" + item.cacheKey())).thenReturn(Uni.createFrom().item(Optional.of(item)));

        try {
            Uni<TestItem> resultUni = service.process(item);
            UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
            subscriber.awaitItem();

            assertSame(item, subscriber.getItem());
            verify(cacheManager).get("v1:" + item.cacheKey());
        } finally {
            PipelineContextHolder.clear();
        }
    }

    private void setPolicy(CacheService<?> service, String policy) throws Exception {
        Field policyField = CacheService.class.getDeclaredField("policyValue");
        policyField.setAccessible(true);
        policyField.set(service, policy);
    }

    private static final class TestItem implements CacheKey {
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
