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

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.pipelineframework.persistence.PersistenceManager;
import org.pipelineframework.plugin.api.PluginReactiveUnary;

/**
 * A general-purpose persistence plugin that can persist any entity that has a corresponding
 * PersistenceProvider configured in the system.
 */
@ApplicationScoped
public class GenericPersistencePlugin implements PluginReactiveUnary<Object> {
    private static final Logger LOG = Logger.getLogger(GenericPersistencePlugin.class);

    private final PersistenceManager persistenceManager;

    /**
     * Create a GenericPersistencePlugin using the supplied persistence manager.
     *
     * @param persistenceManager manager used to persist entities
     */
    public GenericPersistencePlugin(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    /**
     * Persists the provided entity using the configured PersistenceManager.
     *
     * @param item the entity to persist; if null the method returns immediately without attempting persistence
     * @return a Uni that completes with no value when persistence has finished; completes immediately if the item is null
     */
    @Override
    public Uni<Void> process(Object item) {
        LOG.debugf("About to persist entity: %s", item != null ? item.getClass().getName() : "null");
        if (item == null) {
            LOG.warn("Received null item to persist, returning completed Uni");
            return Uni.createFrom().voidItem();
        }

        return persistenceManager.persist(item)
            .onItem().invoke(result -> LOG.debugf("Successfully persisted entity: %s", result != null ? result.getClass().getName() : "null"))
            .onFailure().invoke(failure -> LOG.error("Failed to persist entity", failure))
            .replaceWithVoid();
    }
}