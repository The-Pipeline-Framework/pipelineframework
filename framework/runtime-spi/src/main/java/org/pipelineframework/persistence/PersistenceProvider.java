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

package org.pipelineframework.persistence;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Abstraction for persistence operations that can work with different database technologies.
 *
 * @param <T> The type of entity to persist
 */
public interface PersistenceProvider<T> {

    /**
     * Gets the type of entity handled by this persistence provider.
     *
     * @return The Class object representing the entity type
     */
    Class<T> type();

    /**
     * Persist an entity and return a Uni that completes when the operation is done.
     *
     * @param entity The entity to persist
     * @return A Uni that completes with the persisted entity
     */
    Uni<T> persist(T entity);

    /**
     * Persist or update an entity, if supported by the underlying provider.
     *
     * <p>Default implementation delegates to {@link #persist(Object)}.</p>
     *
     * @param entity The entity to persist or update
     * @return A Uni that completes with the persisted or updated entity
     */
    default Uni<T> persistOrUpdate(T entity) {
        return persist(entity);
    }

    /**
 * Determine whether this provider can handle the given entity instance.
 *
 * @param entity the entity instance to evaluate; its runtime type is used to decide compatibility
 * @return `true` if the provider can persist the entity's runtime type, `false` otherwise
 */
    boolean supports(Object entity);

    /**
         * Indicates whether the provider supports the current thread context (virtual or platform threads).
         *
         * <p>Default implementation returns {@code true} to preserve backward compatibility.</p>
         *
         * @return {@code true} if the provider supports the current thread context, {@code false} otherwise
         */
    default boolean supportsThreadContext() {
        // Default implementation returns true to maintain backward compatibility
        return true;
    }

    /**
     * Indicates whether this provider is safe to invoke concurrently.
     *
     * <p>Default implementation returns {@code SAFE} to preserve backward compatibility.</p>
     *
     * @return the thread safety declaration for this provider
     */
    default ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }
}
