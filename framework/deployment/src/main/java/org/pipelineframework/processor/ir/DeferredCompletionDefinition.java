/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.processor.ir;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.ClassName;

/**
 * Typed compiler input for durable deferred completion attached to an authored operation.
 *
 * <p>The authored operation produces {@code operationOutput}; the pipeline-visible output is
 * supplied later by the configured completion transport.</p>
 */
public record DeferredCompletionDefinition(
    String operationOutputType,
    Optional<ClassName> operationOutputJavaType,
    String timeout,
    List<String> idempotencyKeyFields,
    String correlationStrategy,
    InitiationDefinition initiation,
    Optional<CompletionProjectionDefinition> completion
) {
    public DeferredCompletionDefinition {
        operationOutputType = requireText(operationOutputType, "operationOutput.type");
        operationOutputJavaType = operationOutputJavaType == null ? Optional.empty() : operationOutputJavaType;
        timeout = requireText(timeout, "timeout");
        idempotencyKeyFields = idempotencyKeyFields == null ? List.of() : List.copyOf(idempotencyKeyFields);
        correlationStrategy = requireText(correlationStrategy, "correlation.strategy");
        Objects.requireNonNull(initiation, "initiation");
        completion = completion == null ? Optional.empty() : completion;
    }

    public DeferredCompletionDefinition(String operationOutputType, Optional<ClassName> operationOutputJavaType,
        String timeout, List<String> idempotencyKeyFields, String correlationStrategy, String transportType,
        Map<String, Object> transportConfig, Optional<CompletionProjectionDefinition> completion) {
        this(operationOutputType, operationOutputJavaType, timeout, idempotencyKeyFields, correlationStrategy,
            new PostOperationTransportDefinition(transportType, transportConfig), completion);
    }

    public sealed interface InitiationDefinition permits PostOperationTransportDefinition, ConnectorCallbackDefinition { }

    public record PostOperationTransportDefinition(String type, Map<String, Object> config)
        implements InitiationDefinition {
        public PostOperationTransportDefinition {
            type = requireText(type, "transport.type");
            config = Map.copyOf(Objects.requireNonNull(config, "transport.config"));
        }
    }

    public record ConnectorCallbackDefinition(String name, ClassName endpointResolverClass, ClassName authenticatorClass)
        implements InitiationDefinition {
        public ConnectorCallbackDefinition {
            name = org.pipelineframework.connector.ConnectorProviderId.of(name).value();
            Objects.requireNonNull(endpointResolverClass, "callback.endpointResolver");
            Objects.requireNonNull(authenticatorClass, "callback.authenticator");
        }
    }

    public Optional<ConnectorCallbackDefinition> callback() {
        return initiation instanceof ConnectorCallbackDefinition callback ? Optional.of(callback) : Optional.empty();
    }

    public String transportType() {
        return initiation instanceof PostOperationTransportDefinition transport ? transport.type() : "";
    }

    public Map<String, Object> transportConfig() {
        return initiation instanceof PostOperationTransportDefinition transport ? transport.config() : Map.of();
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }

    /** Request-aware projection from an untrusted completion payload to the step output. */
    public record CompletionProjectionDefinition(String type, ClassName projector) {
        public CompletionProjectionDefinition {
            type = requireText(type, "completion.type");
            Objects.requireNonNull(projector, "completion.projector must not be null");
        }
    }
}
