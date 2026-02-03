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

package org.pipelineframework;

import java.util.concurrent.SubmissionPublisher;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Multi;
import org.jboss.logging.Logger;

/**
 * In-memory output bus for streaming pipeline results to subscribers.
 * Intended for live subscriptions; no replay or durability guarantees.
 *
 * <p>The default {@link SubmissionPublisher} uses {@code ForkJoinPool.commonPool()} and a buffer size of 256.
 * When the buffer is full, {@link SubmissionPublisher#submit(Object)} can block until demand is available.
 * For high-throughput scenarios, consider constructing a publisher with a dedicated executor and a larger buffer.</p>
 */
@ApplicationScoped
@Unremovable
public class PipelineOutputBus implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(PipelineOutputBus.class);

    private final SubmissionPublisher<Object> publisher = new SubmissionPublisher<>();

    /**
     * Publish an object to live subscribers.
     *
     * Null values are ignored and a warning is logged when a null is provided.
     *
     * @param item the object to publish to subscribers
     */
    public void publish(Object item) {
        if (item == null) {
            logger.warn("Ignoring null item published to PipelineOutputBus.");
            return;
        }
        publisher.submit(item);
    }

    /**
     * Create a Mutiny Multi backed by the internal SubmissionPublisher.
     *
     * @return a Multi that emits items published to the internal SubmissionPublisher
     */
    private Multi<Object> stream() {
        return Multi.createFrom().publisher(publisher);
    }

    /**
     * Subscribe to the live output stream and receive only items assignable to the given type.
     *
     * @param type the class of items to subscribe to
     * @param <T> the expected output type
     * @return a Multi that emits items of the specified type
     */
    public <T> Multi<T> stream(Class<T> type) {
        return stream()
            .select()
            .where(type::isInstance)
            .onItem()
            .transform(type::cast);
    }

    /**
     * Closes the output bus and releases its underlying publisher resources.
     *
     * <p>This signals completion to any live subscribers by closing the internal publisher
     * and frees associated resources used for publishing.</p>
     */
    @Override
    @PreDestroy
    public void close() {
        publisher.close();
    }
}