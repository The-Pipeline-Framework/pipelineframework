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

package org.pipelineframework.reject;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Step-level item reject sink SPI.
 */
public interface ItemRejectSink {

    /**
     * Provider name used for configuration-based selection.
     *
     * @return provider name
     */
    default String providerName() {
        return "log";
    }

    /**
     * Provider priority used when multiple sinks are available.
     * Higher numeric values have higher precedence and are selected over lower values.
     *
     * @return provider priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Whether this sink is durable enough for production reject recovery paths.
     *
     * @return true when sink is durable
     */
    default boolean durable() {
        return false;
    }

    /**
     * Validates provider readiness for startup.
     *
     * <p>Return a non-empty value when the provider is selected but cannot safely operate
     * with the current runtime configuration.</p>
     *
     * @param config reject sink configuration
     * @return optional startup validation error
     */
    default Optional<String> startupValidationError(ItemRejectConfig config) {
        return Optional.empty();
    }

    /**
     * Publishes one reject envelope.
     *
     * @param envelope reject envelope
     * @return completion signal
     */
    Uni<Void> publish(ItemRejectEnvelope envelope);
}
