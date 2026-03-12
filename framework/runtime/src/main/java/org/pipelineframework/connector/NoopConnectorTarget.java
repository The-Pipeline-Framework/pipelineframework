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
import org.jboss.logging.Logger;

/**
 * Drains a connector stream without forwarding it.
 *
 * @param <T> target payload type
 */
public final class NoopConnectorTarget<T> implements ConnectorTarget<T> {
    private static final Logger LOG = Logger.getLogger(NoopConnectorTarget.class);
    private static final NoopConnectorTarget<?> INSTANCE = new NoopConnectorTarget<>();

    /**
     * Prevents external instantiation; obtain the singleton via {@link #instance()}.
     */
    private NoopConnectorTarget() {
    }

    /**
     * Provides the singleton NoopConnectorTarget instance parameterized for type `T`.
     *
     * @param <T> the connector record payload type
     * @return the singleton NoopConnectorTarget&lt;T&gt; instance
     */
    @SuppressWarnings("unchecked")
    public static <T> NoopConnectorTarget<T> instance() {
        return (NoopConnectorTarget<T>) INSTANCE;
    }

    /**
     * Subscribes to and drains the provided connector stream, discarding all records.
     *
     * <p>On subscription the stream is consumed without forwarding items; failures are logged
     * at warning level and completion is logged at debug level.</p>
     *
     * @param connectorStream the stream of connector records to consume and discard
     * @return a {@code Cancellable} representing the subscription
     */
    @Override
    public Cancellable forward(Multi<ConnectorRecord<T>> connectorStream) {
        return connectorStream.subscribe().with(
            ignored -> {
            },
            failure -> LOG.warn("Noop connector target failed", failure),
            () -> LOG.debug("Noop connector target completed"));
    }
}
