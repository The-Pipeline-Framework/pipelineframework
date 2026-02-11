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

    @Override
    public Uni<List<O>> emitMany(Multi<TraceEnvelope<O>> items, FunctionTransportContext context) {
        Objects.requireNonNull(items, "items must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return items.onItem().transform(envelope -> envelope == null ? null : envelope.payload()).collect().asList();
    }
}
