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
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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

    @Inject
    Instance<PersistenceProvider<?>> providerInstance;

    @ConfigProperty(name = "pipeline.persistence.provider.class")
    Optional<String> persistenceProviderClass;

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
     * Persist the given entity using a registered persistence provider that supports it and the current thread context.
     *
     * @param <T> the type of entity to persist
     * @param entity the entity to persist
     * @return the persisted entity if a suitable provider handled it, otherwise the original entity; if the input was null the Uni emits `null`
     */
    @WithTransaction
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
    @WithTransaction
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
     * Determine whether configured providers are safe for concurrent access.
     *
     * @return {@code SAFE} if all providers declare SAFE, otherwise {@code UNSAFE}
     */
    public ThreadSafety threadSafety() {
        if (providers == null || providers.isEmpty()) {
            return ThreadSafety.SAFE;
        }
        if (persistenceProviderClass != null && persistenceProviderClass.isPresent() &&
            !persistenceProviderClass.get().isBlank()) {
            PersistenceProvider<?> provider = resolveConfiguredProvider(persistenceProviderClass.get().trim());
            warnIfThreadSafetyDefault(provider);
            return provider.threadSafety();
        }
        providers.forEach(this::warnIfThreadSafetyDefault);
        boolean allSafe = providers.stream()
            .allMatch(provider -> provider.threadSafety() == ThreadSafety.SAFE);
        return allSafe ? ThreadSafety.SAFE : ThreadSafety.UNSAFE;
    }

    /**
     * Determine ordering requirements based on configured providers.
     *
     * @return the provider ordering requirement, or {@code RELAXED} if no provider hint is available
     */
    public OrderingRequirement orderingRequirement() {
        if (providers == null || providers.isEmpty()) {
            return OrderingRequirement.RELAXED;
        }
        if (persistenceProviderClass != null && persistenceProviderClass.isPresent() &&
            !persistenceProviderClass.get().isBlank()) {
            PersistenceProvider<?> provider = resolveConfiguredProvider(persistenceProviderClass.get().trim());
            return orderingRequirementFor(provider, OrderingRequirement.RELAXED);
        }
        if (providers.size() == 1) {
            return orderingRequirementFor(providers.get(0), OrderingRequirement.RELAXED);
        }
        return OrderingRequirement.RELAXED;
    }

    private PersistenceProvider<?> resolveProvider(Object entity) {
        if (providers == null || providers.isEmpty()) {
            LOG.warn("No persistence providers available");
            return null;
        }
        if (persistenceProviderClass != null && persistenceProviderClass.isPresent() &&
            !persistenceProviderClass.get().isBlank()) {
            PersistenceProvider<?> provider = resolveConfiguredProvider(persistenceProviderClass.get().trim());
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

    private PersistenceProvider<?> resolveConfiguredProvider(String configuredClass) {
        for (PersistenceProvider<?> provider : providers) {
            Class<?> providerClass = provider.getClass();
            if (providerClass.getName().equals(configuredClass) ||
                providerClass.getSimpleName().equals(configuredClass)) {
                return provider;
            }
        }
        throw new IllegalStateException(
            "No persistence provider matches pipeline.persistence.provider.class=" + configuredClass);
    }

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
