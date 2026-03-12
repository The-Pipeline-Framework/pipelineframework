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

/**
 * Semantic outcome returned to broker adapters.
 */
public enum BrokerAckDecision {
    /**
     * Terminal success outcome.
     * The broker message has been accepted and should not be delivered again.
     */
    ACK,

    /**
     * Non-terminal retry outcome.
     * The adapter should requeue, reschedule, or back off according to broker-native retry semantics.
     */
    RETRY,

    /**
     * Terminal reject outcome.
     * The adapter should stop retrying and route the message to the broker's reject/DLQ path when available.
     */
    REJECT
}
