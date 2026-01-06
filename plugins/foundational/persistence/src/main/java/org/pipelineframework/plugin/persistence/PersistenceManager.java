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
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.Unremovable;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
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

    @Inject
    Instance<PersistenceProvider<?>> providerInstance;

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
        for (PersistenceProvider<?> provider : providers) {
            if (!provider.supports(entity)) continue;

            // Check if the provider supports the current thread context
            if (!provider.supportsThreadContext()) continue;

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
        for (PersistenceProvider<?> provider : providers) {
            if (!provider.supports(entity)) continue;

            // Check if the provider supports the current thread context
            if (!provider.supportsThreadContext()) continue;

            @SuppressWarnings("unchecked")
            PersistenceProvider<T> p = (PersistenceProvider<T>) provider;
            LOG.debugf("About to persist or update with provider: %s", provider.getClass().getName());

            return p.persistOrUpdate(entity);
        }

        LOG.warnf("No persistence provider found for %s", entity.getClass().getName());
        return Uni.createFrom().item(entity);
    }
}
