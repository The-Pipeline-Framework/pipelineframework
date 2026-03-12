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

package org.pipelineframework.config.connector;

import java.util.Objects;

/**
 * Optional broker-specific connector source details.
 *
 * @param provider broker family, for example SQS, Kafka, or RabbitMQ
 * @param destination queue or topic identifier
 * @param adapter adapter class that maps broker messages into connector records
 */
public record ConnectorBrokerConfig(
    String provider,
    String destination,
    String adapter
) {
    public ConnectorBrokerConfig {
        requireText(provider, "provider");
        requireText(destination, "destination");
        requireText(adapter, "adapter");
    }

    /**
     * Validate that a text value is non-null and contains at least one non-whitespace character.
     *
     * @param value the text to validate
     * @param fieldName the field name used in exception messages
     * @throws NullPointerException if {@code value} is null
     * @throws IllegalArgumentException if {@code value} contains only whitespace
     */
    private static void requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
