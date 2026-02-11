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

/**
 * Converts external function events to a reactive envelope stream.
 *
 * @param <E> source event type (for example API Gateway event, queue batch, or custom trigger)
 * @param <I> pipeline input payload type
 */
public interface FunctionSourceAdapter<E, I> {

    /**
     * Convert an incoming source event into the reactive stream consumed by the pipeline.
     *
     * @param event source event
     * @param context function transport context
     * @return reactive stream of trace envelopes
     */
    Multi<TraceEnvelope<I>> adapt(E event, FunctionTransportContext context);
}

