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

package org.pipelineframework.transport.function;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Converts pipeline output envelopes to a function egress target.
 *
 * @param <O> pipeline output type
 * @param <R> sink acknowledgement/result type
 */
public interface FunctionSinkAdapter<O, R> {

    /**
     * Emits a single envelope.
     *
     * @param item output envelope
     * @param context sink context
     * @return sink ack/result
     */
    default Uni<R> emitOne(TraceEnvelope<O> item, FunctionTransportContext context) {
        return emitMany(Multi.createFrom().item(item), context);
    }

    /**
     * Emits a stream of envelopes.
     *
     * @param items output stream
     * @param context sink context
     * @return sink ack/result
     */
    Uni<R> emitMany(Multi<TraceEnvelope<O>> items, FunctionTransportContext context);
}

