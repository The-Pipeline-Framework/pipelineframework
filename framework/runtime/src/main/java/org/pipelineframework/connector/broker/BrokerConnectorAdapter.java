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

package org.pipelineframework.connector.broker;

import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;
import org.pipelineframework.connector.ConnectorRecord;

/**
 * Maps broker-native messages into connector records and consumes semantic outcomes.
 *
 * @param <M> broker-native message type
 * @param <T> connector payload type
 */
public interface BrokerConnectorAdapter<M, T> {

    /**
     * Converts a broker-native message into a connector record for downstream handoff.
     *
     * @param message the non-null broker-native message to map
     * @return the mapped non-null ConnectorRecord containing the connector payload
     * @throws RuntimeException if the broker message is invalid or cannot be mapped
     */
    @Nonnull
    ConnectorRecord<T> toRecord(@Nonnull M message);

    /**
     * Applies the semantic connector outcome back to the broker transport.
     *
     * @param message broker-native message being acknowledged; must not be null
     * @param decision semantic outcome to apply; must not be null
     * @param failure terminal or retryable failure associated with the decision; null for {@link BrokerAckDecision#ACK}
     */
    void handleDecision(@Nonnull M message, @Nonnull BrokerAckDecision decision, @Nullable Throwable failure);
}
