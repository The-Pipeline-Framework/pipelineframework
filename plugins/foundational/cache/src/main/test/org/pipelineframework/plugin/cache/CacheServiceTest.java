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
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class CacheServiceTest {

    @Test
    void process_WithNullItem_ShouldReturnNull() {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheKeyResolver cacheKeyResolver = mock(CacheKeyResolver.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);

        Uni<TestItem> resultUni = service.process((TestItem) null);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertNull(subscriber.getItem());
    }

    @Test
    void process_WithNonCacheKey_ShouldReturnSameItem() {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheKeyResolver cacheKeyResolver = mock(CacheKeyResolver.class);
        CacheService<Object> service = new CacheService<>(cacheManager, cacheKeyResolver);

        Object item = new Object();
        when(cacheKeyResolver.resolveKey(eq(item), any())).thenReturn(Optional.empty());
        Uni<Object> resultUni = service.process(item);
        UniAssertSubscriber<Object> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verifyNoInteractions(cacheManager);
    }

    @Test
    void process_WithCacheOnlyPolicy_ShouldCache() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-1");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "cache-only");

        when(cacheManager.cache(keyFor(item), item)).thenReturn(Uni.createFrom().item(item));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager).cache(keyFor(item), item);
    }

    @Test
    void process_WithReturnCachedPolicy_ShouldReturnCachedWhenPresent() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-2");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "return-cached");
        TestItem cached = new TestItem("id-2");
        when(cacheManager.get(keyFor(item))).thenReturn(Uni.createFrom().item(Optional.of(cached)));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(cached, subscriber.getItem());
        verify(cacheManager, never()).cache(any(), any());
    }

    @Test
    void process_WithReturnCachedPolicy_ShouldCacheOnMiss() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-3");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "return-cached");
        when(cacheManager.get(keyFor(item))).thenReturn(Uni.createFrom().item(Optional.empty()));
        when(cacheManager.cache(keyFor(item), item)).thenReturn(Uni.createFrom().item(item));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager).cache(keyFor(item), item);
    }

    @Test
    void process_WithSkipIfPresentPolicy_ShouldSkipWhenExists() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-4");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "skip-if-present");
        when(cacheManager.exists(keyFor(item))).thenReturn(Uni.createFrom().item(true));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager, never()).cache(any(), any());
    }

    @Test
    void process_WithSkipIfPresentPolicy_ShouldNotCacheWhenMissing() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-5");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "skip-if-present");
        when(cacheManager.exists(keyFor(item))).thenReturn(Uni.createFrom().item(false));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager, never()).cache(any(), any());
    }

    @Test
    void process_WithBypassCachePolicy_ShouldSkipReadAndWrite() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        CacheKeyResolver cacheKeyResolver = mock(CacheKeyResolver.class);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "bypass-cache");

        TestItem item = new TestItem("id-bypass");

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verifyNoInteractions(cacheManager);
    }

    @Test
    void process_WithRequireCachePolicy_ShouldReturnCachedWhenPresent() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-req");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "require-cache");
        TestItem cached = new TestItem("id-req");
        when(cacheManager.get(keyFor(item))).thenReturn(Uni.createFrom().item(Optional.of(cached)));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(cached, subscriber.getItem());
        verify(cacheManager, never()).cache(any(), any());
    }

    @Test
    void process_WithRequireCachePolicy_ShouldReturnItemOnMiss() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-miss");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "require-cache");
        when(cacheManager.get(keyFor(item))).thenReturn(Uni.createFrom().item(Optional.empty()));

        Uni<TestItem> resultUni = service.process(item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(cacheManager, never()).cache(any(), any());
    }

    @Test
    void process_WithContextPolicyOverride_ShouldUseOverride() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-6");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "cache-only");
        PipelineContextHolder.set(new PipelineContext(null, null, "return-cached"));
        when(cacheManager.get(keyFor(item))).thenReturn(Uni.createFrom().item(Optional.of(item)));

        try {
            Uni<TestItem> resultUni = service.process(item);
            UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
            subscriber.awaitItem();

            assertSame(item, subscriber.getItem());
            verify(cacheManager).get(keyFor(item));
            verify(cacheManager, never()).cache(any(), any());
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void process_WithVersionTag_ShouldPrefixCacheKey() throws Exception {
        CacheManager cacheManager = mock(CacheManager.class);
        TestItem item = new TestItem("id-7");
        CacheKeyResolver cacheKeyResolver = resolverFor(item);
        CacheService<TestItem> service = new CacheService<>(cacheManager, cacheKeyResolver);
        setPolicy(service, "return-cached");
        PipelineContextHolder.set(new PipelineContext("v1", null, null));
        when(cacheManager.get("v1:" + keyFor(item))).thenReturn(Uni.createFrom().item(Optional.of(item)));

        try {
            Uni<TestItem> resultUni = service.process(item);
            UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
            subscriber.awaitItem();

            assertSame(item, subscriber.getItem());
            verify(cacheManager).get("v1:" + keyFor(item));
        } finally {
            PipelineContextHolder.clear();
        }
    }

    private void setPolicy(CacheService<?> service, String policy) throws Exception {
        Field policyField = CacheService.class.getDeclaredField("policyValue");
        policyField.setAccessible(true);
        policyField.set(service, policy);
    }

    private String keyFor(TestItem item) {
        return "key-" + item.id;
    }

    private CacheKeyResolver resolverFor(TestItem item) {
        CacheKeyResolver resolver = mock(CacheKeyResolver.class);
        when(resolver.resolveKey(eq(item), any())).thenReturn(Optional.of(keyFor(item)));
        return resolver;
    }

    private static final class TestItem {
        private final String id;

        private TestItem(String id) {
            this.id = id;
        }
    }
}
