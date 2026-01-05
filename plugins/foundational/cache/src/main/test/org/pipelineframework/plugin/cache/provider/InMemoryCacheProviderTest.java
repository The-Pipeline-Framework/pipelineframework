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

package org.pipelineframework.plugin.cache.provider;

import java.time.Duration;
import java.util.Optional;

import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheKey;

import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryCacheProviderTest {

    @Test
    void cache_WithTtl_ShouldExpireEntry() throws InterruptedException {
        InMemoryCacheProvider provider = new InMemoryCacheProvider();
        TestItem item = new TestItem("id-1");

        provider.cache(item.cacheKey(), item, Duration.ofMillis(10))
            .subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem();

        Optional<CacheKey> cached = provider.get(item.cacheKey())
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
        assertTrue(cached.isPresent());

        Thread.sleep(20);

        Optional<CacheKey> expired = provider.get(item.cacheKey())
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
        assertTrue(expired.isEmpty());
    }

    @Test
    void invalidateByPrefix_ShouldRemoveMatchingEntries() {
        InMemoryCacheProvider provider = new InMemoryCacheProvider();
        TestItem itemA = new TestItem("typeA:1");
        TestItem itemB = new TestItem("typeA:2");
        TestItem itemC = new TestItem("typeB:1");

        provider.cache(itemA.cacheKey(), itemA).subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem();
        provider.cache(itemB.cacheKey(), itemB).subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem();
        provider.cache(itemC.cacheKey(), itemC).subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem();

        Boolean removed = provider.invalidateByPrefix("typeA:")
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

        assertTrue(removed);
        assertTrue(provider.get(itemA.cacheKey())
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem().isEmpty());
        assertTrue(provider.get(itemB.cacheKey())
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem().isEmpty());
        assertTrue(provider.get(itemC.cacheKey())
            .subscribe().withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem().isPresent());
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
