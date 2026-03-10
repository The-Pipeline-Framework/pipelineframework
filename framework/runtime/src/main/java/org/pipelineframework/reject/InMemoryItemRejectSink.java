/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.reject;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Bounded in-memory reject sink provider for local and development triage.
 */
@ApplicationScoped
public class InMemoryItemRejectSink implements ItemRejectSink {

    private static final Logger LOG = Logger.getLogger(InMemoryItemRejectSink.class);

    @Inject
    ItemRejectConfig itemRejectConfig;

    private final Object lock = new Object();
    private final ArrayDeque<ItemRejectEnvelope> ring = new ArrayDeque<>();

    /**
     * Identifies this sink implementation as the in-memory provider.
     *
     * @return the provider name "memory"
     */
    @Override
    public String providerName() {
        return "memory";
    }

    /**
     * Indicates this sink's selection priority among available providers.
     *
     * @return `-200` indicating a lower priority relative to providers with higher values.
     */
    @Override
    public int priority() {
        return -200;
    }

    /**
     * Validates configuration required for the in-memory reject sink.
     *
     * @param config the item-reject configuration to validate
     * @return an Optional containing an error message if `config` is null or `memoryCapacity()` is less than or equal to zero, or `Optional.empty()` when the configuration is valid
     */
    @Override
    public Optional<String> startupValidationError(ItemRejectConfig config) {
        if (config == null || config.memoryCapacity() <= 0) {
            return Optional.of("pipeline.item-reject.memory-capacity must be > 0 when provider=memory.");
        }
        return Optional.empty();
    }

    /**
     * Stores the given reject envelope in the bounded in-memory sink, evicting oldest entries when the configured
     * capacity is reached, and records metrics for the stored envelope.
     *
     * @param envelope the item reject envelope to store
     * @return an empty result whose completion indicates the envelope has been added to the in-memory buffer and metrics recorded
     */
    @Override
    public Uni<Void> publish(ItemRejectEnvelope envelope) {
        return Uni.createFrom().voidItem().invoke(() -> {
            int capacity = Math.max(1, itemRejectConfig.memoryCapacity());
            int retained;
            synchronized (lock) {
                while (ring.size() >= capacity) {
                    ring.removeFirst();
                }
                ring.addLast(envelope);
                retained = ring.size();
            }
            ItemRejectMetrics.record(providerName(), envelope);
            LOG.warnf(
                "Item reject stored in memory sink: step=%s scope=%s fingerprint=%s retained=%d/%d",
                envelope.stepClass(),
                envelope.rejectScope(),
                envelope.itemFingerprint(),
                retained,
                capacity);
        });
    }

    /**
     * Create an immutable snapshot of the current in-memory reject envelopes.
     *
     * @return an immutable List of ItemRejectEnvelope containing the sink's current contents in insertion order (oldest first)
     */
    public List<ItemRejectEnvelope> snapshot() {
        synchronized (lock) {
            return List.copyOf(ring);
        }
    }

}
