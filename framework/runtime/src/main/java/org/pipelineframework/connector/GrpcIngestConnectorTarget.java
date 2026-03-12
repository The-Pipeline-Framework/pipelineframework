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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Generic gRPC-style ingest connector target backed by an existing typed ingest client.
 *
 * @param <T> target payload type
 */
public final class GrpcIngestConnectorTarget<T> implements ConnectorTarget<T> {
    private final Function<Multi<T>, Cancellable> forwarder;

    /**
     * Creates a GrpcIngestConnectorTarget that forwards payload streams using the provided forwarder.
     *
     * @param forwarder function that accepts a {@code Multi<T>} of payloads and returns a {@code Cancellable} to control the forwarding subscription
     * @throws NullPointerException if {@code forwarder} is {@code null}
     */
    public GrpcIngestConnectorTarget(Function<Multi<T>, Cancellable> forwarder) {
        this.forwarder = Objects.requireNonNull(forwarder, "forwarder must not be null");
    }

    /**
     * Forwards the payloads from the given connector record stream to the configured forwarder.
     *
     * @param connectorStream stream of ConnectorRecord whose payloads will be extracted and forwarded
     * @return a Cancellable that can be used to cancel the forwarding subscription
     * @throws NullPointerException if {@code connectorStream} is null
     */
    @Override
    public Cancellable forward(Multi<ConnectorRecord<T>> connectorStream) {
        Objects.requireNonNull(connectorStream, "connectorStream must not be null");
        return forwarder.apply(connectorStream.onItem().transform(ConnectorRecord::payload));
    }
}
