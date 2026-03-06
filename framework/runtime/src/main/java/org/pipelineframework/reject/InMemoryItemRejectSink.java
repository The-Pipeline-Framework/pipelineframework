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

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -200;
    }

    @Override
    public Optional<String> startupValidationError(ItemRejectConfig config) {
        if (config == null || config.memoryCapacity() <= 0) {
            return Optional.of("pipeline.item-reject.memory-capacity must be > 0 when provider=memory.");
        }
        return Optional.empty();
    }

    @Override
    public Uni<Void> publish(ItemRejectEnvelope envelope) {
        return Uni.createFrom().voidItem().invoke(() -> {
            int capacity = Math.max(1, itemRejectConfig.memoryCapacity());
            synchronized (lock) {
                while (ring.size() >= capacity) {
                    ring.removeFirst();
                }
                ring.addLast(envelope);
            }
            ItemRejectMetrics.record(providerName(), envelope);
            LOG.warnf(
                "Item reject stored in memory sink: step=%s scope=%s fingerprint=%s retained=%d/%d",
                envelope.stepClass(),
                envelope.rejectScope(),
                envelope.itemFingerprint(),
                currentSize(),
                capacity);
        });
    }

    /**
     * Returns a snapshot of current in-memory rejects.
     *
     * @return immutable snapshot
     */
    public List<ItemRejectEnvelope> snapshot() {
        synchronized (lock) {
            return List.copyOf(ring);
        }
    }

    private int currentSize() {
        synchronized (lock) {
            return ring.size();
        }
    }
}
