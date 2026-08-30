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

package org.pipelineframework.connector;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * A finite streaming Query observation and its provider-resource lifetime.
 *
 * <p>The publisher carries row demand and cancellation. {@code termination} completes only after
 * the publisher has terminated or been cancelled and all resources owned by the provider have
 * been released. The publisher must be finite, respect downstream demand, and preserve a stable
 * total row order whenever the same logical Query expansion is re-evaluated. TPF derives child
 * identity from that order; pagination and fetch windows must not be observable as items.</p>
 */
public record QueryStream<O>(Flow.Publisher<O> rows, CompletionStage<Void> termination) {
    public QueryStream {
        rows = Objects.requireNonNull(rows, "query stream rows must not be null");
        termination = Objects.requireNonNull(termination, "query stream termination must not be null");
    }
}
