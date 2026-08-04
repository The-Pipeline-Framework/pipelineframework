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

package org.pipelineframework.plugin.persistence;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.quarkus.arc.ClientProxy;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.persistence.PersistenceProvider;

/**
 * Manager for persistence operations that delegates to registered PersistenceProvider implementations.
 *
 * <p>This class handles persistence operations by identifying an appropriate provider based on the
 * type of entity and thread context, then delegating the operation to that provider.</p>
 */
@ApplicationScoped
@Unremovable
public class PersistenceManager {

    private static final Logger LOG = Logger.getLogger(PersistenceManager.class);

    private List<PersistenceProvider<?>> providers;
    private final Set<Class<?>> warnedOrderingDefaults = ConcurrentHashMap.newKeySet();
    private final Set<Class<?>> warnedThreadSafetyDefaults = ConcurrentHashMap.newKeySet();
    private volatile String cachedConfiguredProviderKey;
    private volatile PersistenceProvider<?> cachedConfiguredProvider;

    @Inject
    Instance<PersistenceProvider<?>> providerInstance;

    @Inject
    PersistenceConfig persistenceConfig;

    /**
     * Default constructor for PersistenceManager.
     */
    public PersistenceManager() {
    }

    @PostConstruct
    void init() {
        LOG.debug("PersistenceManager init() called");
        LOG.debugf("Provider instance available: %s", providerInstance != null);

        if (providerInstance != null) {
            this.providers = providerInstance.stream().toList();
            LOG.infof("Initialised %s persistence providers", providers.size());
            for (int i = 0; i < providers.size(); i++) {
                LOG.debugf("Provider %d: %s", i, providers.get(i).getClass().getName());
            }
        } else {
            LOG.warn("providerInstance is null!");
            this.providers = List.of();
        }
    }

    /**
     * Persist the given entity using a registered persistence provider that supports the entity and the current thread context.
     *
     * @param <T> the type of the entity
     * @param entity the entity to persist
     * @return the persisted entity if a suitable provider handled it, the original entity if no provider matched, or `null` if the input was `null`
     */
    public <T> Uni<T> persist(T entity) {
        if (entity == null) {
            LOG.debug("Entity is null, returning empty Uni");
            return Uni.createFrom().nullItem();
        }

        LOG.debugf("Entity to persist: %s", entity.getClass().getName());
        PersistenceProvider<?> provider = resolveProvider(entity);
        if (provider != null) {
            @SuppressWarnings("unchecked")
            PersistenceProvider<T> p = (PersistenceProvider<T>) provider;
            LOG.debugf("About to persist with provider: %s", provider.getClass().getName());
            return p.persist(entity);
        }

        LOG.warnf("No persistence provider found for %s", entity.getClass().getName());
        return Uni.createFrom().item(entity);
    }

    /**
     * Persist or update the given entity using a registered persistence provider that supports it and the current thread context.
     *
     * @param <T> the type of entity to persist or update
     * @param entity the entity to persist or update
     * @return the persisted entity if a suitable provider handled it, otherwise the original entity; if the input was null the Uni emits `null`
     */
    public <T> Uni<T> persistOrUpdate(T entity) {
        if (entity == null) {
            LOG.debug("Entity is null, returning empty Uni");
            return Uni.createFrom().nullItem();
        }

        LOG.debugf("Entity to persist or update: %s", entity.getClass().getName());
        PersistenceProvider<?> provider = resolveProvider(entity);
        if (provider != null) {
            @SuppressWarnings("unchecked")
            PersistenceProvider<T> p = (PersistenceProvider<T>) provider;
            LOG.debugf("About to persist or update with provider: %s", provider.getClass().getName());
            return p.persistOrUpdate(entity);
        }

        LOG.warnf("No persistence provider found for %s", entity.getClass().getName());
        return Uni.createFrom().item(entity);
    }

    /**
     * Determine the aggregate thread-safety requirement of the registered persistence providers.
     *
     * @return {@code SAFE} if all applicable providers report {@code SAFE}, {@code UNSAFE} otherwise
     * @throws IllegalStateException if a configured provider class is present but cannot be resolved to a matching provider
     */
    public ThreadSafety threadSafety() {
        if (providers == null || providers.isEmpty()) {
            return ThreadSafety.SAFE;
        }
        if (hasConfiguredProvider()) {
            PersistenceProvider<?> provider = resolveConfiguredProvider(configuredProviderClass().get().trim());
            warnIfThreadSafetyDefault(provider);
            return provider.threadSafety();
        }
        providers.forEach(this::warnIfThreadSafetyDefault);
        boolean allSafe = providers.stream()
            .allMatch(provider -> provider.threadSafety() == ThreadSafety.SAFE);
        return allSafe ? ThreadSafety.SAFE : ThreadSafety.UNSAFE;
    }

    /**
         * Determine the ordering requirement to use for persistence operations.
         *
         * If a provider class is configured, the configured provider's ordering hint is used (falling back to
         * RELAXED if the provider does not declare a hint). If no configured provider is present and exactly one
         * provider is available, that provider's hint is used (falling back to RELAXED if absent). In all other
         * cases RELAXED is assumed.
         *
         * @return the ordering requirement to apply for persistence operations; one of the enum values (typically a
         *         provider-specific requirement or {@code RELAXED} when no hint is available)
         * @throws IllegalStateException if a provider class is configured but cannot be resolved to a registered provider
         */
    public OrderingRequirement orderingRequirement() {
        if (providers == null || providers.isEmpty()) {
            return OrderingRequirement.RELAXED;
        }
        if (hasConfiguredProvider()) {
            PersistenceProvider<?> provider = resolveConfiguredProvider(configuredProviderClass().get().trim());
            return orderingRequirementFor(provider, OrderingRequirement.RELAXED);
        }
        if (providers.size() == 1) {
            return orderingRequirementFor(providers.get(0), OrderingRequirement.RELAXED);
        }
        return OrderingRequirement.RELAXED;
    }

    /**
     * Selects the most appropriate PersistenceProvider for the given entity and current thread context.
     *
     * If a provider is explicitly configured, validates and returns that provider; otherwise returns the
     * first registered provider that supports the entity and current thread context. Returns `null` when
     * no suitable provider is available.
     *
     * @param entity the persistence entity used to determine provider support; may not be null when callers expect a match
     * @return the matching PersistenceProvider, or `null` if none is available
     * @throws IllegalStateException if a configured provider is set but does not support the entity or the current thread context
     */
    private PersistenceProvider<?> resolveProvider(Object entity) {
        if (providers == null || providers.isEmpty()) {
            LOG.warn("No persistence providers available");
            return null;
        }
        if (hasConfiguredProvider()) {
            PersistenceProvider<?> provider = resolveConfiguredProvider(configuredProviderClass().get().trim());
            if (!provider.supports(entity)) {
                throw new IllegalStateException(
                    "Configured persistence provider " + provider.getClass().getName() +
                        " does not support entity " + entity.getClass().getName());
            }
            if (!provider.supportsThreadContext()) {
                throw new IllegalStateException(
                    "Configured persistence provider " + provider.getClass().getName() +
                        " does not support the current thread context");
            }
            return provider;
        }
        for (PersistenceProvider<?> provider : providers) {
            if (!provider.supports(entity)) {
                continue;
            }
            if (!provider.supportsThreadContext()) {
                continue;
            }
            return provider;
        }
        return null;
    }

    /**
     * Resolve the configured provider class name to a registered PersistenceProvider.
     *
     * @param configuredClass the configured provider class name; may be a fully-qualified name,
     *                        a simple class name, or null/blank
     * @return the registered PersistenceProvider whose implementation class matches the configured name
     * @throws IllegalStateException if no registered provider matches the configured name; the exception
     *         message lists discovered providers and guidance for configuring a valid provider class
     */
    private PersistenceProvider<?> resolveConfiguredProvider(String configuredClass) {
        String configured = configuredClass == null ? "" : configuredClass.trim();
        PersistenceProvider<?> cached = cachedConfiguredProvider;
        if (cached != null && configured.equals(cachedConfiguredProviderKey)) {
            return cached;
        }
        synchronized (this) {
            cached = cachedConfiguredProvider;
            if (cached != null && configured.equals(cachedConfiguredProviderKey)) {
                return cached;
            }
            PersistenceProvider<?> resolved = resolveConfiguredProviderUncached(configured, configuredClass);
            cachedConfiguredProvider = resolved;
            cachedConfiguredProviderKey = configured;
            return resolved;
        }
    }

    private PersistenceProvider<?> resolveConfiguredProviderUncached(String configured, String configuredClass) {
        boolean simpleNameConfigured = !configured.contains(".");
        for (PersistenceProvider<?> provider : providers) {
            Object unwrapped = provider instanceof ClientProxy
                ? ClientProxy.unwrap(provider)
                : provider;
            Class<?> providerClass = unwrapped != null ? unwrapped.getClass() : provider.getClass();
            if (providerClass.getName().equals(configured)
                || providerClass.getName().equals(configured + "_Subclass")) {
                return provider;
            }
            if (simpleNameConfigured
                && (providerClass.getSimpleName().equals(configured)
                    || providerClass.getSimpleName().equals(configured + "_Subclass"))) {
                return provider;
            }
            if (!simpleNameConfigured && isAssignableToConfigured(providerClass, configured)) {
                return provider;
            }
        }
        String available = providers.stream()
            .map(this::providerClassName)
            .distinct()
            .sorted()
            .toList()
            .toString();
        throw new IllegalStateException("Configured persistence.provider.class='" + configuredClass
            + "' does not match any discovered provider. Available providers: " + available
            + ". Set persistence.provider.class to a discovered provider FQCN (or simple class name), "
            + "or remove the override to allow automatic provider resolution.");
    }

    /**
     * Retrieve the configured persistence provider class name if one is set.
     *
     * @return an Optional containing the configured provider class name, or empty if no provider class is configured
     */
    private Optional<String> configuredProviderClass() {
        if (persistenceConfig == null) {
            return Optional.empty();
        }
        return persistenceConfig.providerClass();
    }

    /**
     * Whether a provider class has been configured in persistence settings.
     *
     * @return true if a non-blank configured provider class is present, false otherwise.
     */
    private boolean hasConfiguredProvider() {
        Optional<String> configuredProvider = configuredProviderClass();
        return configuredProvider.isPresent() && !configuredProvider.get().isBlank();
    }

    /**
     * Get the provider's concrete class name, unwrapping CDI proxies when present.
     *
     * @param provider the PersistenceProvider instance or a CDI proxy; may be null
     * @return the fully qualified class name of the underlying provider, or the string "null" if {@code provider} is null
     */
    private String providerClassName(PersistenceProvider<?> provider) {
        if (provider == null) {
            return "null";
        }
        if (provider instanceof ClientProxy proxy) {
            Object unwrapped = ClientProxy.unwrap(proxy);
            if (unwrapped != null) {
                return unwrapped.getClass().getName();
            }
        }
        return provider.getClass().getName();
    }

    /**
     * Checks whether the given provider class is assignable to the configured class specified by name.
     *
     * Resolves the configured class name (using the thread context ClassLoader if available) and
     * returns whether the configured class is assignable from the provider class.
     *
     * @param providerClass  the provider's runtime Class to test
     * @param configuredClass  the fully qualified name of the configured class to compare against
     * @return true if the configured class can be assigned from the provider class; false if not or if the configured class name cannot be resolved
     */
    private boolean isAssignableToConfigured(Class<?> providerClass, String configuredClass) {
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Class<?> configured = contextClassLoader != null
                ? Class.forName(configuredClass, false, contextClassLoader)
                : Class.forName(configuredClass);
            return configured.isAssignableFrom(providerClass);
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    /**
     * Determine the ordering requirement for the given persistence provider, using the provided fallback when the provider is null or does not declare a @ParallelismHint.
     *
     * @param provider the persistence provider whose @ParallelismHint (if present) will be used
     * @param fallback the ordering to return if the provider is null or has no @ParallelismHint
     * @return the provider's ordering from its @ParallelismHint, or `fallback` if none is present or `provider` is null
     */
    private OrderingRequirement orderingRequirementFor(PersistenceProvider<?> provider,
                                                       OrderingRequirement fallback) {
        if (provider == null) {
            return fallback;
        }
        ParallelismHint hint = provider.getClass().getAnnotation(ParallelismHint.class);
        if (hint == null) {
            warnIfOrderingDefault(provider);
            return fallback;
        }
        return hint.ordering();
    }

    private void warnIfOrderingDefault(PersistenceProvider<?> provider) {
        Class<?> providerClass = provider.getClass();
        if (warnedOrderingDefaults.add(providerClass)) {
            LOG.warnf("Persistence provider %s does not declare @ParallelismHint; assuming RELAXED ordering.",
                providerClass.getName());
        }
    }

    private void warnIfThreadSafetyDefault(PersistenceProvider<?> provider) {
        try {
            var method = provider.getClass().getMethod("threadSafety");
            if (method.getDeclaringClass().equals(PersistenceProvider.class)
                && warnedThreadSafetyDefaults.add(provider.getClass())) {
                LOG.warnf("Persistence provider %s does not override threadSafety(); assuming SAFE.",
                    provider.getClass().getName());
            }
        } catch (NoSuchMethodException ignored) {
            // No warning; fallback to provider implementation.
        }
    }
}