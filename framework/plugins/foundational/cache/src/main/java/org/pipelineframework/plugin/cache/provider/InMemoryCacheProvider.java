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
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.cache.CacheProvider;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Simple in-memory cache provider for arbitrary items.
 */
@ApplicationScoped
@Unremovable
@IfBuildProperty(name = "pipeline.cache.provider", stringValue = "memory")
@ParallelismHint(ordering = OrderingRequirement.RELAXED, threadSafety = ThreadSafety.SAFE)
public class InMemoryCacheProvider implements CacheProvider<Object> {

    private static final Logger LOG = Logger.getLogger(InMemoryCacheProvider.class);

    private final ConcurrentMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    /**
     * Default constructor for InMemoryCacheProvider.
     */
    public InMemoryCacheProvider() {
    }

    @Override
    public Class<Object> type() {
        return Object.class;
    }

    @Override
    public Uni<Object> cache(String key, Object value) {
        return cache(key, value, null);
    }

    @Override
    public Uni<Object> cache(String key, Object value, Duration ttl) {
        if (key == null || key.isBlank()) {
            LOG.warn("Cache key is null or blank, skipping cache");
            return Uni.createFrom().item(value);
        }
        cache.put(key, new CacheEntry(value, expiresAt(ttl)));
        LOG.debugf("Cached item with key: %s", key);
        return Uni.createFrom().item(value);
    }

    @Override
    public Uni<Optional<Object>> get(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(Optional.empty());
        }
        CacheEntry entry = cache.get(key);
        if (entry == null || entry.isExpired()) {
            cache.remove(key);
            return Uni.createFrom().item(Optional.empty());
        }
        return Uni.createFrom().item(Optional.of(entry.value()));
    }

    @Override
    public Uni<Boolean> exists(String key) {
        return get(key).map(Optional::isPresent);
    }

    @Override
    public Uni<Boolean> invalidate(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(false);
        }
        return Uni.createFrom().item(cache.remove(key) != null);
    }

    @Override
    public Uni<Boolean> invalidateByPrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return Uni.createFrom().item(false);
        }
        int removed = 0;
        for (String key : cache.keySet()) {
            if (key.startsWith(prefix)) {
                cache.remove(key);
                removed++;
            }
        }
        return Uni.createFrom().item(removed > 0);
    }

    @Override
    public String backend() {
        return "memory";
    }

    @Override
    public boolean supports(Object item) {
        return true;
    }

    @Override
    public ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }

    private Instant expiresAt(Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return null;
        }
        return Instant.now().plus(ttl);
    }

    private record CacheEntry(Object value, Instant expiresAt) {
        boolean isExpired() {
            return expiresAt != null && Instant.now().isAfter(expiresAt);
        }
    }
}
