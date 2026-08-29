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

import java.util.Optional;

/**
 * Host-neutral finite streaming Query operation contract.
 *
 * <p>{@link java.util.concurrent.Flow.Publisher} is the provider boundary because it expresses
 * demand and cancellation without imposing a reactive-library dependency on provider code. Each
 * invocation supplies one ordered, finite row observation; the runtime subscribes it once.</p>
 */
public interface StreamingQueryOperation<I, C, O> extends ConnectorOperation {
    default Optional<ConnectorConfigSchema<C>> configurationSchema() {
        return Optional.empty();
    }

    QueryStream<O> query(QueryInvocation<I, C, O> invocation);

    default QueryStream<O> query(
        I input,
        ConnectorConfigurationDocument configuration,
        Class<O> outputType,
        ConnectorExecutionContext executionContext
    ) {
        C boundConfiguration = configurationSchema()
            .map(schema -> ConnectorConfigurationBinder.bind(schema, configuration, "streaming query operation " + id()))
            .orElseGet(() -> zeroConfiguration(configuration));
        return query(new QueryInvocation<>(input, boundConfiguration, outputType, executionContext));
    }

    @SuppressWarnings("unchecked")
    private C zeroConfiguration(ConnectorConfigurationDocument configuration) {
        if (!configuration.values().isEmpty()) {
            throw new ConnectorConfigurationException(
                "streaming query operation " + id() + " does not declare a configuration schema");
        }
        return (C) ConnectorConfigurationDocument.empty();
    }
}
