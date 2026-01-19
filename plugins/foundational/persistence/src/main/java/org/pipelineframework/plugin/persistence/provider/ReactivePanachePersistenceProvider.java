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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.persistence.Entity;
import jakarta.persistence.PersistenceException;

import io.quarkus.arc.Unremovable;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.persistence.PersistenceProvider;

/**
 * Reactive persistence provider using Hibernate Reactive Panache.
 */
@ApplicationScoped
@Unremovable
@ParallelismHint(ordering = OrderingRequirement.STRICT_ADVISED, threadSafety = ThreadSafety.SAFE)
public class ReactivePanachePersistenceProvider implements PersistenceProvider<Object> {

  private static final Logger LOG = Logger.getLogger(ReactivePanachePersistenceProvider.class);

  /**
   * Default constructor for ReactivePanachePersistenceProvider.
   */
  public ReactivePanachePersistenceProvider() {
  }

  @Override
  public Class<Object> type() {
    return Object.class;
  }

  /**
   * Persists a Panache entity using Hibernate Reactive.
   */
  @Override
  public Uni<Object> persist(Object entity) {
    LOG.tracef("Persisting entity of type %s", entity.getClass().getSimpleName());

    return Panache.getSession()
        .onItem()
        .transformToUni(session -> 
            session.withTransaction(ignored -> session.persist(entity)))
        .replaceWith(Uni.createFrom().item(entity))
        .onFailure()
        .transform(
            t ->
                new PersistenceException(
                    "Failed to persist entity of type " + entity.getClass().getName(), t));
    }

    @Override
    public Uni<Object> persistOrUpdate(Object entity) {
        LOG.tracef("Persisting or updating entity of type %s", entity.getClass().getSimpleName());

        return Panache.getSession()
            .onItem()
            .transformToUni(session ->
                session.withTransaction(ignored -> session.merge(entity)))
            .replaceWith(Uni.createFrom().item(entity))
            .onFailure()
            .transform(
                t ->
                    new PersistenceException(
                        "Failed to persist or update entity of type " + entity.getClass().getName(), t));
    }

    /**
     * Checks whether the provider supports the given entity instance.
     *
     * @return {@code true} if the entity class is annotated with {@code jakarta.persistence.Entity}, {@code false} otherwise.
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
     * Indicate whether this persistence provider supports the current thread context.
     *
     * @return `true` if the current thread is not a virtual thread, `false` otherwise.
     */
    @Override
    public boolean supportsThreadContext() {
        // This provider is designed for reactive (non-virtual) threads
        return !Thread.currentThread().isVirtual();
    }
}
