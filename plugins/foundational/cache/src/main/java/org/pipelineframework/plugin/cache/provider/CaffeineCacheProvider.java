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
import java.util.concurrent.CompletableFuture;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheManager;
import io.quarkus.cache.CaffeineCache;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.cache.CacheProvider;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Caffeine-based cache provider.
 */
@ApplicationScoped
@Unremovable
@IfBuildProperty(name = "pipeline.cache.provider", stringValue = "caffeine")
@ParallelismHint(ordering = OrderingRequirement.RELAXED, threadSafety = ThreadSafety.SAFE)
public class CaffeineCacheProvider implements CacheProvider<Object> {

    private static final Logger LOG = Logger.getLogger(CaffeineCacheProvider.class);

    @ConfigProperty(name = "pipeline.cache.caffeine.name", defaultValue = "pipeline-cache")
    String cacheName;

    @ConfigProperty(name = "pipeline.cache.caffeine.maximum-size", defaultValue = "10000")
    long maximumSize;

    @ConfigProperty(name = "pipeline.cache.caffeine.expire-after-write")
    Optional<Duration> expireAfterWrite;

    @ConfigProperty(name = "pipeline.cache.caffeine.expire-after-access")
    Optional<Duration> expireAfterAccess;

    @Inject
    CacheManager cacheManager;

    private Cache cache;
    private CaffeineCache caffeineCache;

    /**
     * Default constructor for CaffeineCacheProvider.
     */
    public CaffeineCacheProvider() {
    }

    @PostConstruct
    void init() {
        cache = cacheManager.getCache(cacheName)
            .orElseThrow(() -> new IllegalStateException("Cache not found: " + cacheName));
        caffeineCache = cache.as(CaffeineCache.class);
        caffeineCache.setMaximumSize(maximumSize);
        expireAfterWrite.ifPresent(caffeineCache::setExpireAfterWrite);
        expireAfterAccess.ifPresent(caffeineCache::setExpireAfterAccess);
        LOG.debugf("Caffeine cache provider initialised with cache: %s", cacheName);
    }

    @Override
    public Class<Object> type() {
        return Object.class;
    }

    @Override
    public Uni<Object> cache(String key, Object value) {
        if (key == null || key.isBlank()) {
            LOG.warn("Cache key is null or blank, skipping cache");
            return Uni.createFrom().item(value);
        }
        caffeineCache.put(key, CompletableFuture.completedFuture(value));
        return Uni.createFrom().item(value);
    }

    @Override
    public Uni<Object> cache(String key, Object value, Duration ttl) {
        if (ttl != null && !ttl.isZero() && !ttl.isNegative()) {
            caffeineCache.setExpireAfterWrite(ttl);
        }
        return cache(key, value);
    }

    @Override
    public Uni<Optional<Object>> get(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(Optional.empty());
        }
        return Uni.createFrom().completionStage(caffeineCache.getIfPresent(key))
            .onItem().transform(Optional::ofNullable);
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
        return cache.invalidate(key).replaceWith(true);
    }

    @Override
    public Uni<Boolean> invalidateByPrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return Uni.createFrom().item(false);
        }
        LOG.warnf("Prefix invalidation is not supported for Caffeine cache '%s'", cacheName);
        return Uni.createFrom().item(false);
    }

    @Override
    public String backend() {
        return "caffeine";
    }

    @Override
    public boolean supports(Object item) {
        return true;
    }

    @Override
    public ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }
}
