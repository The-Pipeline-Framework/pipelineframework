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

package org.pipelineframework.transport.function;

import java.util.List;
import java.util.Objects;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Sink adapter that collects a stream of envelopes into a list of payloads.
 *
 * @param <O> output payload type
 */
public final class CollectListFunctionSinkAdapter<O> implements FunctionSinkAdapter<O, List<O>> {
    private final BatchingPolicy batchingPolicy;

    public CollectListFunctionSinkAdapter() {
        this(BatchingPolicy.defaultPolicy());
    }

    public CollectListFunctionSinkAdapter(BatchingPolicy batchingPolicy) {
        this.batchingPolicy = Objects.requireNonNull(batchingPolicy, "batchingPolicy must not be null");
    }

    @Override
    public Uni<List<O>> emitMany(Multi<TraceEnvelope<O>> items, FunctionTransportContext context) {
        if (items == null) {
            return Uni.createFrom().failure(new NullPointerException("items must not be null"));
        }
        if (context == null) {
            return Uni.createFrom().failure(new NullPointerException("context must not be null"));
        }
        // Context is currently not consumed by this sink; reserved for future boundary telemetry/extensions.
        return boundedItems(items)
            .select().where(Objects::nonNull)
            .collect().asList()
            .onItem().transform(envelopes -> envelopes.stream()
                .map(TraceEnvelope::payload)
                .toList());
    }

    private Multi<TraceEnvelope<O>> boundedItems(Multi<TraceEnvelope<O>> items) {
        int maxItems = batchingPolicy.maxItems();
        int fetchCount = maxItems == Integer.MAX_VALUE ? maxItems : maxItems + 1;
        return switch (batchingPolicy.overflowPolicy()) {
            case FAIL -> items.select().first(fetchCount).collect().asList()
                .onItem().transformToMulti(collected -> {
                    if (collected.size() > maxItems) {
                        return Multi.createFrom().failure(new IllegalStateException(
                            "Function sink overflow: received at least " + collected.size()
                                + " items with maxItems=" + maxItems + " and overflowPolicy=FAIL"));
                    }
                    return Multi.createFrom().iterable(collected);
                });
            case DROP, BUFFER -> items.select().first(maxItems);
        };
    }
}
