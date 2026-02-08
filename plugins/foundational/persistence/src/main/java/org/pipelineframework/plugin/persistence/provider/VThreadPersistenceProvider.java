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

package org.pipelineframework.plugin.persistence.provider;

import jakarta.enterprise.context.Dependent;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import io.quarkus.arc.Arc;
import io.quarkus.arc.Unremovable;
import io.quarkus.arc.InjectableInstance;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.persistence.PersistenceProvider;

/**
 * A persistence provider implementation that works with virtual threads (using Jakarta Persistence EntityManager).
 * This provider is designed to handle persistence operations within virtual thread contexts.
 */
@Dependent
@Unremovable
@ParallelismHint(ordering = OrderingRequirement.STRICT_ADVISED, threadSafety = ThreadSafety.SAFE)
public class VThreadPersistenceProvider implements PersistenceProvider<Object> {

    private final Logger logger = Logger.getLogger(VThreadPersistenceProvider.class);

    private final InjectableInstance<EntityManager> entityManagerInstance;

    /**
     * Initializes the provider and resolves an injectable EntityManager via Arc.
     *
     * Stores the resolved InjectableInstance<EntityManager> for the provider's persistence operations.
     */
    public VThreadPersistenceProvider() {
        // Look up the EntityManager bean instance via Arc
        entityManagerInstance = Arc.container().select(EntityManager.class);
    }

    /**
     * Persist the provided entity within a JPA transaction.
     *
     * @return the persisted entity instance
     * @throws IllegalStateException if no EntityManager is resolvable for this provider
     */
    @Override
    @Transactional
    public Uni<Object> persist(Object entity) {
        if (entity == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("Cannot persist a null entity"));
        }

        return Uni.createFrom().item(() -> {
            if (!entityManagerInstance.isResolvable()) {
                throw new IllegalStateException("No EntityManager available for VThreadPersistenceProvider");
            }

            EntityManager em = entityManagerInstance.get();
            em.persist(entity);
            return entity;
        });
    }

    /**
     * Merge the provided entity into the persistence context and return the managed instance.
     *
     * @param entity the entity instance to merge; must not be null
     * @return the merged (managed) entity instance
     * @throws IllegalArgumentException if {@code entity} is null
     * @throws IllegalStateException if no {@link javax.persistence.EntityManager} is available
     */
    @Override
    @Transactional
    public Uni<Object> persistOrUpdate(Object entity) {
        if (entity == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("Cannot persist a null entity"));
        }

        return Uni.createFrom().item(() -> {
            if (!entityManagerInstance.isResolvable()) {
                throw new IllegalStateException("No EntityManager available for VThreadPersistenceProvider");
            }

            EntityManager em = entityManagerInstance.get();
            Object mergedEntity = em.merge(entity);
            return mergedEntity;
        });
    }

    /**
     * Identifies the handled entity type for this persistence provider.
     *
     * @return the Class object representing the handled entity type, {@code Object.class}
     */
    @Override
    public Class<Object> type() {
        return Object.class;
    }

    /**
     * Checks whether an object's runtime class is annotated as a JPA entity.
     *
     * @param entity the object whose runtime class will be inspected for the `@Entity` annotation
     * @return `true` if the runtime class of {@code entity} is annotated with `jakarta.persistence.Entity`, `false` otherwise
     */
    @Override
    public boolean supports(Object entity) {
        return entity != null && entity.getClass().isAnnotationPresent(Entity.class);
    }

    /**
     * Indicates that this persistence provider is safe for concurrent use by multiple threads.
     *
     * @return `ThreadSafety.SAFE` when the provider is thread-safe.
     */
    @Override
    public ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }

    /**
         * Determines whether the current thread context is supported for persistence operations.
         *
         * @return {@code true} if running on a virtual thread or on a thread that is not a Vert.x event-loop thread, {@code false} otherwise.
         */
    @Override
    public boolean supportsThreadContext() {
        // Allow virtual threads and non-event-loop threads; reject Vert.x event-loop threads.
        if (Thread.currentThread().isVirtual()) {
            return true;
        }
        return !Context.isOnEventLoopThread();
    }
}