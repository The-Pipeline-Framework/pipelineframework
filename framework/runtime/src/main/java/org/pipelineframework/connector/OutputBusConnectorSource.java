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
import java.util.Objects;
import org.pipelineframework.PipelineOutputBus;

/**
 * Connector source backed by the in-memory pipeline output bus.
 *
 * @param <T> source payload type
 */
public final class OutputBusConnectorSource<T> implements ConnectorSource<T> {
    private final PipelineOutputBus outputBus;
    private final Class<T> sourceType;

    public OutputBusConnectorSource(PipelineOutputBus outputBus, Class<T> sourceType) {
        this.outputBus = Objects.requireNonNull(outputBus, "outputBus must not be null");
        this.sourceType = Objects.requireNonNull(sourceType, "sourceType must not be null");
    }

    @Override
    public Multi<ConnectorRecord<T>> stream() {
        return outputBus.stream(sourceType)
            .onItem().transform(ConnectorRecord::ofPayload);
    }
}
