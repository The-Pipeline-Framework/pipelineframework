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
     * Identifies the provider name used to select this sink via configuration.
     *
     * @return the provider name used for configuration selection
     */
    default String providerName() {
        return "log";
    }

    /**
     * Provider priority used when multiple sinks are available.
     * Higher numeric values have higher precedence and are selected over lower values.
     * The default is intentionally low so sinks that do not override priority do not
     * unintentionally supersede built-in providers.
     *
     * @return an integer priority where higher values indicate higher precedence
     */
    default int priority() {
        return -500;
    }

    /**
     * Indicates whether the sink is durable enough for production reject recovery paths.
     *
     * @return `true` if the sink is durable and suitable for production reject recovery paths, `false` otherwise
     */
    default boolean durable() {
        return false;
    }

    /**
     * Validate whether the provider is ready to operate at startup.
     *
     * <p>When the provider is selected but cannot safely operate with the given runtime
     * configuration, return a non-empty Optional containing a user-facing validation error
     * message; return an empty Optional when no startup error is detected.</p>
     *
     * @param config the reject sink configuration to validate
     * @return an Optional with a startup validation error message if validation fails, empty otherwise
     */
    default Optional<String> startupValidationError(ItemRejectConfig config) {
        return Optional.empty();
    }

    /**
     * Publish a single reject envelope.
     *
     * @param envelope the reject envelope to publish
     * @return a Uni that completes when the envelope has been published; emits no item
     */
    Uni<Void> publish(ItemRejectEnvelope envelope);
}
