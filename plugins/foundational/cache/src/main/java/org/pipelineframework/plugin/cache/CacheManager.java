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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.cache.CacheProvider;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Manager for cache operations that delegates to registered CacheProvider implementations.
 */
@ApplicationScoped
@Unremovable
public class CacheManager {

    private static final Logger LOG = Logger.getLogger(CacheManager.class);

    private List<CacheProvider<?>> providers;
    private final Set<Class<?>> warnedOrderingDefaults = ConcurrentHashMap.newKeySet();
    private final Set<Class<?>> warnedThreadSafetyDefaults = ConcurrentHashMap.newKeySet();

    @Inject
    Instance<CacheProvider<?>> providerInstance;

    @ConfigProperty(name = "pipeline.cache.provider")
    Optional<String> cacheProvider;

    @ConfigProperty(name = "pipeline.cache.provider.class")
    Optional<String> cacheProviderClass;

    @ConfigProperty(name = "quarkus.profile", defaultValue = "prod")
    String profile;

    @ConfigProperty(name = "pipeline.cache.ttl")
    Optional<Duration> cacheTtl;

    /**
     * Default constructor for CacheManager.
     */
    public CacheManager() {
    }

    @PostConstruct
    void init() {
        LOG.debug("CacheManager init() called");
        LOG.debugf("Provider instance available: %s", providerInstance != null);

        if (providerInstance != null) {
            this.providers = providerInstance.stream().toList();
            LOG.infof("Initialised %s cache providers", providers.size());
            for (int i = 0; i < providers.size(); i++) {
                LOG.debugf("Provider %d: %s", i, providers.get(i).getClass().getName());
            }
        } else {
            LOG.warn("providerInstance is null!");
            this.providers = List.of();
        }
    }

    /**
     * Cache the given item using the supplied key and a registered cache provider.
     *
     * @param <T> the type of item to cache
     * @param key cache key to use
     * @param item the item to cache
     * @return the cached item if a suitable provider handled it, otherwise the original item;
     *         if the input was null the Uni emits `null`
     */
    public <T> Uni<T> cache(String key, T item) {
        if (item == null) {
            LOG.debug("Item is null, returning empty Uni");
            return Uni.createFrom().nullItem();
        }
        if (key == null || key.isBlank()) {
            LOG.warnf("Cache key is empty for item type %s, skipping cache", item.getClass().getName());
            return Uni.createFrom().item(item);
        }

        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return Uni.createFrom().item(item);
        }
        if (!provider.supports(item)) {
            LOG.warnf("Cache provider %s does not support %s",
                provider.getClass().getName(), item.getClass().getName());
            return Uni.createFrom().item(item);
        }
        if (!provider.supportsThreadContext()) {
            LOG.warnf("Cache provider %s does not support current thread context",
                provider.getClass().getName());
            return Uni.createFrom().item(item);
        }

        @SuppressWarnings("unchecked")
        CacheProvider<T> p = (CacheProvider<T>) provider;
        LOG.debugf("About to cache with provider: %s", provider.getClass().getName());

        if (cacheTtl.isPresent()) {
            return p.cache(key, item, cacheTtl.get());
        }
        return p.cache(key, item);
    }

    /**
     * Determine if the cache contains the given key.
     *
     * @param key cache key
     * @return true if the key exists, false otherwise
     */
    public Uni<Boolean> exists(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(false);
        }

        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return Uni.createFrom().item(false);
        }
        return provider.exists(key);
    }

    /**
     * Retrieve a cached entry by key if supported.
     *
     * @param key cache key
     * @return the cached item if present
     */
    public <T> Uni<Optional<T>> get(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(Optional.empty());
        }

        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return Uni.createFrom().item(Optional.empty());
        }
        @SuppressWarnings("unchecked")
        CacheProvider<T> p = (CacheProvider<T>) provider;
        return p.get(key);
    }

    /**
     * Invalidate a cached entry by key.
     *
     * @param key cache key
     * @return true if the entry was invalidated, false otherwise
     */
    public Uni<Boolean> invalidate(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(false);
        }

        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return Uni.createFrom().item(false);
        }
        return provider.invalidate(key);
    }

    /**
     * Invalidate cached entries by prefix.
     *
     * @param prefix key prefix to invalidate
     * @return true if entries were invalidated, false otherwise
     */
    public Uni<Boolean> invalidateByPrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return Uni.createFrom().item(false);
        }

        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return Uni.createFrom().item(false);
        }
        return provider.invalidateByPrefix(prefix);
    }

    private CacheProvider<?> resolveProvider() {
        if (providers == null || providers.isEmpty()) {
            LOG.warn("No cache providers available");
            return null;
        }
        if (cacheProviderClass != null && cacheProviderClass.isPresent() && !cacheProviderClass.get().isBlank()) {
            String configuredClass = cacheProviderClass.get().trim();
            for (CacheProvider<?> provider : providers) {
                Class<?> providerClass = provider.getClass();
                if (providerClass.getName().equals(configuredClass) ||
                    providerClass.getSimpleName().equals(configuredClass)) {
                    return provider;
                }
            }
            throw new IllegalStateException(
                "No cache provider matches pipeline.cache.provider.class=" + configuredClass);
        }
        if (cacheProvider != null && cacheProvider.isPresent() && !cacheProvider.get().isBlank()) {
            String configuredProvider = cacheProvider.get();
            for (CacheProvider<?> provider : providers) {
                String backend = provider.backend();
                if (backend != null && configuredProvider.equalsIgnoreCase(backend)) {
                    return provider;
                }
            }
            LOG.warnf("No cache provider matches pipeline.cache.provider=%s", configuredProvider);
            return null;
        }
        if (providers.size() == 1) {
            return providers.get(0);
        }
        if (isNonProdProfile()) {
            CacheProvider<?> provider = providers.get(0);
            LOG.warnf("Multiple cache providers found (%s). No pipeline.cache.provider configured; using %s for profile=%s.",
                providerBackends(), provider.getClass().getName(), profile);
            return provider;
        }
        throw new IllegalStateException(
            "Multiple cache providers found (" + providerBackends() + "). " +
                "Set pipeline.cache.provider explicitly.");
    }

    /**
     * Determine whether configured providers are safe for concurrent access.
     *
     * @return {@code SAFE} if all providers declare SAFE, otherwise {@code UNSAFE}
     */
    public ThreadSafety threadSafety() {
        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return ThreadSafety.SAFE;
        }
        warnIfThreadSafetyDefault(provider);
        return provider.threadSafety();
    }

    /**
     * Determine ordering requirements based on the resolved provider's hint.
     *
     * @return the provider's ordering requirement, or {@code RELAXED} if unspecified
     */
    public OrderingRequirement orderingRequirement() {
        CacheProvider<?> provider = resolveProvider();
        if (provider == null) {
            return OrderingRequirement.RELAXED;
        }
        ParallelismHint hint = provider.getClass().getAnnotation(ParallelismHint.class);
        if (hint == null) {
            warnIfOrderingDefault(provider);
        }
        return hint == null ? OrderingRequirement.RELAXED : hint.ordering();
    }

    private void warnIfOrderingDefault(CacheProvider<?> provider) {
        Class<?> providerClass = provider.getClass();
        if (warnedOrderingDefaults.add(providerClass)) {
            LOG.warnf("Cache provider %s does not declare @ParallelismHint; assuming RELAXED ordering.",
                providerClass.getName());
        }
    }

    private void warnIfThreadSafetyDefault(CacheProvider<?> provider) {
        try {
            var method = provider.getClass().getMethod("threadSafety");
            if (method.getDeclaringClass().equals(CacheProvider.class)
                && warnedThreadSafetyDefaults.add(provider.getClass())) {
                LOG.warnf("Cache provider %s does not override threadSafety(); assuming SAFE.",
                    provider.getClass().getName());
            }
        } catch (NoSuchMethodException ignored) {
            // No warning; fallback to provider implementation.
        }
    }

    private boolean isNonProdProfile() {
        if (profile == null) {
            return false;
        }
        String normalized = profile.trim().toLowerCase();
        return normalized.equals("dev") || normalized.equals("test");
    }

    private String providerBackends() {
        return providers.stream()
            .map(provider -> {
                String backend = provider.backend();
                return backend == null || backend.isBlank()
                    ? provider.getClass().getSimpleName()
                    : backend;
            })
            .distinct()
            .sorted()
            .reduce((left, right) -> left + ", " + right)
            .orElse("unknown");
    }
}
