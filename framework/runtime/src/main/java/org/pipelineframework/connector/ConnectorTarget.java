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
import java.util.function.Consumer;

/**
 * Consumes connector records and forwards them into the next boundary.
 *
 * @param <T> target payload type
 */
@FunctionalInterface
public interface ConnectorTarget<T> {

    Cancellable forward(Multi<ConnectorRecord<T>> connectorStream);

    default Cancellable forward(
        Multi<ConnectorRecord<T>> connectorStream,
        Consumer<ConnectorRecord<T>> onAccepted,
        Consumer<Throwable> onFailure
    ) {
        Objects.requireNonNull(connectorStream, "connectorStream must not be null");
        Objects.requireNonNull(onAccepted, "onAccepted must not be null");
        Objects.requireNonNull(onFailure, "onFailure must not be null");
        Multi<ConnectorRecord<T>> wrapped = connectorStream
            .onItem().invoke(onAccepted)
            .onFailure().invoke(onFailure);
        return forward(wrapped);
    }
}
