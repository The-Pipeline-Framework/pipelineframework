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
@ParallelismHint(ordering = OrderingRequirement.STRICT_ADVISED, threadSafety = ThreadSafety.UNSAFE)
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
   * Persist the provided JPA entity within a reactive transaction.
   *
   * @param entity the JPA entity to persist
   * @return the same entity instance after persistence
   */
  @Override
  public Uni<Object> persist(Object entity) {
    LOG.tracef("Persisting entity of type %s", entity.getClass().getSimpleName());

    return Panache.withTransaction(
            () ->
                Panache.getSession()
                    .onItem()
                    .transformToUni(session -> session.persist(entity).replaceWith(entity)))
        .onFailure()
        .transform(
            t ->
                new PersistenceException(
                    "Failed to persist entity of type " + entity.getClass().getName(), t));
  }

  /**
   * Persists a new entity or merges an existing entity within a reactive Panache transaction.
   *
   * <p>On failure, the operation results in a PersistenceException that wraps the original cause.
   *
   * @param entity the JPA entity instance to persist or merge
   * @return the managed entity after persist or merge
   */
  @Override
  public Uni<Object> persistOrUpdate(Object entity) {
    LOG.tracef("Persisting or updating entity of type %s", entity.getClass().getSimpleName());

    return Panache.withTransaction(
            () ->
                Panache.getSession()
                    .onItem()
                    .transformToUni(session -> session.merge(entity)))
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

    /**
     * Report the provider's thread-safety level.
     *
     * @return `ThreadSafety.UNSAFE` indicating the provider is not safe for concurrent use across threads.
     */
    @Override
    public ThreadSafety threadSafety() {
        return ThreadSafety.UNSAFE;
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