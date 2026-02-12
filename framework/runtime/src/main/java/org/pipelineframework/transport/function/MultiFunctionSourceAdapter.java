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

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import io.smallrye.mutiny.Multi;

/**
 * Source adapter for streaming ingress events represented as {@code Multi<I>}.
 *
 * @param <I> payload type
 */
public final class MultiFunctionSourceAdapter<I> implements FunctionSourceAdapter<Multi<I>, I> {
    private static final Logger LOG = Logger.getLogger(MultiFunctionSourceAdapter.class.getName());

    private final String payloadModel;
    private final String payloadModelVersion;
    private final BatchingPolicy batchingPolicy;

    /**
     * Creates an adapter.
     *
     * @param payloadModel payload model
     * @param payloadModelVersion payload model version
     */
    public MultiFunctionSourceAdapter(String payloadModel, String payloadModelVersion) {
        this(payloadModel, payloadModelVersion, BatchingPolicy.defaultPolicy());
    }

    public MultiFunctionSourceAdapter(String payloadModel, String payloadModelVersion, BatchingPolicy batchingPolicy) {
        this.payloadModel = AdapterUtils.normalizeOrDefault(payloadModel, "unknown.input");
        this.payloadModelVersion = AdapterUtils.normalizeOrDefault(payloadModelVersion, "v1");
        this.batchingPolicy = Objects.requireNonNull(batchingPolicy, "batchingPolicy must not be null");
    }

    @Override
    public Multi<TraceEnvelope<I>> adapt(Multi<I> event, FunctionTransportContext context) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(context, "context must not be null");
        String traceId = AdapterUtils.deriveTraceId(context.requestId());
        Map<String, String> immutableMeta = Map.copyOf(AdapterUtils.buildContextMeta(context));
        AtomicLong outputIndex = new AtomicLong(0);

        return boundedIngress(event).onItem().transform(item -> {
            String idempotencyKey = resolveIdempotencyKey(
                context,
                traceId,
                outputIndex.getAndIncrement());
            return TraceEnvelope.rootWithMeta(
                traceId,
                UUID.randomUUID().toString(),
                payloadModel,
                payloadModelVersion,
                idempotencyKey,
                item,
                immutableMeta
            );
        });
    }

    private Multi<I> boundedIngress(Multi<I> event) {
        int maxItems = batchingPolicy.maxItems();
        return switch (batchingPolicy.overflowPolicy()) {
            case FAIL -> event.select().first(maxItems + 1).collect().asList()
                .onItem().transformToMulti(collected -> {
                    if (collected.size() > maxItems) {
                        return Multi.createFrom().failure(new IllegalStateException(
                            "Function source overflow detected with overflowPolicy=FAIL: received more than "
                                + maxItems + " items; collected at least " + collected.size()));
                    }
                    return Multi.createFrom().iterable(collected);
                });
            case DROP -> event.select().first(maxItems);
            // BUFFER intentionally matches DROP for ingress because source-side buffering is owned by upstream/event source.
            case BUFFER -> event.select().first(maxItems);
        };
    }

    private String resolveIdempotencyKey(
            FunctionTransportContext context,
            String traceId,
            long index) {
        if (context.idempotencyPolicy() == IdempotencyPolicy.EXPLICIT
                && context.explicitIdempotencyKey().isEmpty()) {
            LOG.warning(() -> "IdempotencyPolicy.EXPLICIT configured but no explicit key provided; "
                + "falling back to CONTEXT_STABLE key derivation.");
        }
        String suffix = Long.toString(index);
        return IdempotencyKeyResolver.resolve(context, traceId, payloadModel, suffix, suffix);
    }

}
