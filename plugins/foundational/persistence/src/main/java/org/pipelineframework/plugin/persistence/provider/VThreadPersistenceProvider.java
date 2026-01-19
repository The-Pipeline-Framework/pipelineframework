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

import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableInstance;
import io.smallrye.mutiny.Uni;
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
@ParallelismHint(ordering = OrderingRequirement.STRICT_ADVISED, threadSafety = ThreadSafety.SAFE)
public class VThreadPersistenceProvider implements PersistenceProvider<Object> {

  private final Logger logger = Logger.getLogger(VThreadPersistenceProvider.class);

  private final InjectableInstance<EntityManager> entityManagerInstance;

  /**
   * Initialises the provider and locates an injectable EntityManager instance via Arc.
   *
   * Stores the resolved InjectableInstance&lt;EntityManager&gt; for use by the provider's persistence operations.
   */
  public VThreadPersistenceProvider() {
    // Look up the EntityManager bean instance via Arc
    entityManagerInstance = Arc.container().select(EntityManager.class);
  }

  /**
   * Persist the given entity within a JPA transaction and return the same instance.
   *
   * @param entity the entity to persist
   * @return the persisted entity instance
   * @throws IllegalStateException if no EntityManager is resolvable for this provider
   */
  @Override
  public Uni<Object> persist(Object entity) {
    if (entity == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("Cannot persist a null entity"));
    }
    
    return Uni.createFrom().item(() -> {
        if (!entityManagerInstance.isResolvable()) {
        throw new IllegalStateException("No EntityManager available for VThreadPersistenceProvider");
        }

        try (EntityManager em = entityManagerInstance.get()) {
            em.getTransaction().begin();
            try {
                em.persist(entity);
                em.getTransaction().commit();
            } catch (Exception e) {
                if (em.getTransaction().isActive()) {
                    em.getTransaction().rollback();
                }
                throw e;
            }
            return entity;
        }
    });
  }

  @Override
  public Uni<Object> persistOrUpdate(Object entity) {
    if (entity == null) {
      return Uni.createFrom().failure(new IllegalArgumentException("Cannot persist a null entity"));
    }

    return Uni.createFrom().item(() -> {
        if (!entityManagerInstance.isResolvable()) {
        throw new IllegalStateException("No EntityManager available for VThreadPersistenceProvider");
        }

        try (EntityManager em = entityManagerInstance.get()) {
            em.getTransaction().begin();
            try {
                em.merge(entity);
                em.getTransaction().commit();
            } catch (Exception e) {
                if (em.getTransaction().isActive()) {
                    em.getTransaction().rollback();
                }
                throw e;
            }
            return entity;
        }
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

  @Override
  public ThreadSafety threadSafety() {
    return ThreadSafety.SAFE;
  }

  /**
   * Indicates whether this provider is running in a virtual-thread context.
   *
   * @return {@code true} if the current thread is a virtual thread, {@code false} otherwise.
   */
  @Override
  public boolean supportsThreadContext() {
    // This provider is designed for virtual threads
    return Thread.currentThread().isVirtual();
  }
}
