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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Canonical pipeline connector declaration shared by runtime and deployment loaders.
 *
 * @param name connector id
 * @param enabled whether the connector should start at runtime
 * @param source connector source details
 * @param target connector target details
 * @param mapper optional mapper bean class implementing the connector mapper contract
 * @param transport connector transport family
 * @param idempotency idempotency policy name
 * @param backpressure backpressure policy name
 * @param failureMode failure handling mode name
 * @param backpressureBufferCapacity buffer capacity used when backpressure is BUFFER
 * @param idempotencyMaxKeys in-memory accepted/in-flight key window size
 * @param idempotencyKeyFields stable payload fields used to derive handoff keys when none are present
 * @param broker optional broker source details
 */
public record ConnectorConfig(
    String name,
    boolean enabled,
    ConnectorSourceConfig source,
    ConnectorTargetConfig target,
    String mapper,
    String transport,
    String idempotency,
    String backpressure,
    String failureMode,
    int backpressureBufferCapacity,
    int idempotencyMaxKeys,
    List<String> idempotencyKeyFields,
    ConnectorBrokerConfig broker
) {

    public ConnectorConfig {
        Objects.requireNonNull(name, "name must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(target, "target must not be null");
        if (backpressureBufferCapacity < 0) {
            throw new IllegalArgumentException("backpressureBufferCapacity must be >= 0");
        }
        if (idempotencyMaxKeys < 0) {
            throw new IllegalArgumentException("idempotencyMaxKeys must be >= 0");
        }
        idempotencyMaxKeys = idempotencyMaxKeys > 0 ? idempotencyMaxKeys : 10000;
        idempotencyKeyFields = idempotencyKeyFields == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(idempotencyKeyFields));
    }
}
