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
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CacheManagerTest {

    private CacheManager cacheManager;

    @BeforeEach
    void setUp() {
        cacheManager = new CacheManager();
    }

    @Test
    void cache_WithNullItem_ShouldReturnNull() {
        Uni<Object> resultUni = cacheManager.cache("key-0", null);
        UniAssertSubscriber<Object> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();
        assertNull(subscriber.getItem());
    }

    @Test
    void cache_WithBlankKey_ShouldReturnSameItem() throws Exception {
        Object item = new Object();
        setProviders(List.of(mock(CacheProvider.class)));

        Uni<Object> resultUni = cacheManager.cache(" ", item);
        UniAssertSubscriber<Object> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
    }

    @Test
    void cache_WithSupportedProvider_ShouldUseProvider() throws Exception {
        TestItem item = new TestItem("id-1");
        String key = "key-1";
        CacheProvider<TestItem> provider = mock(CacheProvider.class);
        when(provider.backend()).thenReturn("memory");
        when(provider.supports(item)).thenReturn(true);
        when(provider.supportsThreadContext()).thenReturn(true);
        when(provider.cache(eq(key), eq(item)))
            .thenReturn(Uni.createFrom().item(item));

        setProviders(List.of(provider));
        setConfig("memory", Optional.empty());

        Uni<TestItem> resultUni = cacheManager.cache(key, item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(provider).cache(eq(key), eq(item));
    }

    @Test
    void cache_WithProviderClass_ShouldUseMatchingProvider() throws Exception {
        TestItem item = new TestItem("id-1a");
        String key = "key-1a";
        RecordingProvider providerA = new RecordingProvider("a");
        RecordingProvider providerB = new RecordingProvider("b");

        setProviders(List.of(providerA, providerB));
        setConfig("memory", Optional.empty());
        setProviderClass(providerB.getClass().getName());

        Uni<TestItem> resultUni = cacheManager.cache(key, item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        assertFalse(providerA.used());
        assertTrue(providerB.used());
    }

    @Test
    void cache_WithTtl_ShouldUseTtlCacheMethod() throws Exception {
        TestItem item = new TestItem("id-2");
        String key = "key-2";
        CacheProvider<TestItem> provider = mock(CacheProvider.class);
        when(provider.backend()).thenReturn("memory");
        when(provider.supports(item)).thenReturn(true);
        when(provider.supportsThreadContext()).thenReturn(true);
        when(provider.cache(eq(key), eq(item), any(Duration.class)))
            .thenReturn(Uni.createFrom().item(item));

        setProviders(List.of(provider));
        setConfig("memory", Optional.of(Duration.ofSeconds(1)));

        Uni<TestItem> resultUni = cacheManager.cache(key, item);
        UniAssertSubscriber<TestItem> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(item, subscriber.getItem());
        verify(provider).cache(eq(key), eq(item), any(Duration.class));
        verify(provider, never()).cache(eq(key), eq(item));
    }

    @Test
    void exists_UsesSelectedProvider() throws Exception {
        CacheProvider<TestItem> provider = mock(CacheProvider.class);
        when(provider.backend()).thenReturn("memory");
        when(provider.exists("key-1")).thenReturn(Uni.createFrom().item(true));

        setProviders(List.of(provider));
        setConfig("memory", Optional.empty());

        Uni<Boolean> resultUni = cacheManager.exists("key-1");
        UniAssertSubscriber<Boolean> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertTrue(subscriber.getItem());
    }

    @Test
    void get_UsesSelectedProvider() throws Exception {
        TestItem item = new TestItem("id-3");
        CacheProvider<TestItem> provider = mock(CacheProvider.class);
        when(provider.backend()).thenReturn("memory");
        when(provider.get("key-2")).thenReturn(Uni.createFrom().item(Optional.of(item)));

        setProviders(List.of(provider));
        setConfig("memory", Optional.empty());

        Uni<Optional<TestItem>> resultUni = cacheManager.get("key-2");
        UniAssertSubscriber<Optional<TestItem>> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertTrue(subscriber.getItem().isPresent());
        assertSame(item, subscriber.getItem().get());
    }

    @Test
    void invalidate_UsesSelectedProvider() throws Exception {
        CacheProvider<TestItem> provider = mock(CacheProvider.class);
        when(provider.backend()).thenReturn("memory");
        when(provider.invalidate("key-3")).thenReturn(Uni.createFrom().item(true));

        setProviders(List.of(provider));
        setConfig("memory", Optional.empty());

        Uni<Boolean> resultUni = cacheManager.invalidate("key-3");
        UniAssertSubscriber<Boolean> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertTrue(subscriber.getItem());
    }

    @Test
    void invalidateByPrefix_UsesSelectedProvider() throws Exception {
        CacheProvider<TestItem> provider = mock(CacheProvider.class);
        when(provider.backend()).thenReturn("memory");
        when(provider.invalidateByPrefix("prefix-")).thenReturn(Uni.createFrom().item(true));

        setProviders(List.of(provider));
        setConfig("memory", Optional.empty());

        Uni<Boolean> resultUni = cacheManager.invalidateByPrefix("prefix-");
        UniAssertSubscriber<Boolean> subscriber = resultUni.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertTrue(subscriber.getItem());
    }

    private void setProviders(List<CacheProvider<?>> providers) throws Exception {
        Field providersField = CacheManager.class.getDeclaredField("providers");
        providersField.setAccessible(true);
        providersField.set(cacheManager, providers);
    }

    private void setConfig(String provider, Optional<Duration> ttl) throws Exception {
        Field providerField = CacheManager.class.getDeclaredField("cacheProvider");
        providerField.setAccessible(true);
        providerField.set(cacheManager, Optional.ofNullable(provider));

        Field providerClassField = CacheManager.class.getDeclaredField("cacheProviderClass");
        providerClassField.setAccessible(true);
        providerClassField.set(cacheManager, Optional.empty());

        Field ttlField = CacheManager.class.getDeclaredField("cacheTtl");
        ttlField.setAccessible(true);
        ttlField.set(cacheManager, ttl);

        Field profileField = CacheManager.class.getDeclaredField("profile");
        profileField.setAccessible(true);
        profileField.set(cacheManager, "test");
    }

    private void setProviderClass(String providerClass) throws Exception {
        Field providerClassField = CacheManager.class.getDeclaredField("cacheProviderClass");
        providerClassField.setAccessible(true);
        providerClassField.set(cacheManager, Optional.ofNullable(providerClass));
    }

    private static final class TestItem {
        private final String id;

        private TestItem(String id) {
            this.id = id;
        }
    }

    private static final class RecordingProvider implements CacheProvider<TestItem> {
        private final String backend;
        private boolean used;

        private RecordingProvider(String backend) {
            this.backend = backend;
        }

        @Override
        public Class<TestItem> type() {
            return TestItem.class;
        }

        @Override
        public Uni<TestItem> cache(String key, TestItem value) {
            used = true;
            return Uni.createFrom().item(value);
        }

        @Override
        public String backend() {
            return backend;
        }

        @Override
        public boolean supports(Object item) {
            return item instanceof TestItem;
        }

        private boolean used() {
            return used;
        }
    }
}
