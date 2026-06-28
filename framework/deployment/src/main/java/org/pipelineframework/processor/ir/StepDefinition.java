/*
 * Copyright (c) 2023-2025 Mariano Barcia
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
import javax.annotation.Nullable;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateStepExecution;

/**
 * Represents a step definition parsed from YAML configuration.
 * This model drives the generation of step clients based on YAML declarations.
 *
 * @param name The name of the step
 * @param kind The kind of step (INTERNAL, DELEGATED, REMOTE, AWAIT, COMMAND, or QUERY)
 * @param executionClass The class that provides the execution implementation
 * @param delegatedMethodName Optional delegated/operator method name when YAML uses Class::method syntax
 * @param remoteExecution remote execution metadata for REMOTE steps
 * @param awaitConfig raw await configuration for AWAIT steps
 * @param timeout await timeout string for AWAIT steps
 * @param idempotencyKeyFields fields used to derive await idempotency keys
 * @param command command connector name for COMMAND steps
 * @param commandIdGenerator command id generator class for COMMAND steps
 * @param duplicatePolicy duplicate policy for COMMAND steps
 * @param commandConfig connector config for COMMAND steps
 * @param queryId referenced query id for QUERY steps
 * @param queryConfig raw capture configuration for QUERY steps
 * @param queryKeyFields fields used to derive captured query keys
 * @param inboundMapper The inbound mapper class for internal service steps (nullable)
 * @param outboundMapper The outbound mapper class for internal service steps (nullable)
 * @param externalMapper The operator mapper class for mapping between domain and operator types (nullable)
 * @param mapperFallback mapper fallback strategy when no mapper matches (never null)
 * @param inputType The input type for the step
 * @param outputType The output type for the step
 * @param streamingShapeHint Optional streaming shape hint parsed from YAML cardinality
 * @param runOnVirtualThreads Whether blocking execution should use virtual threads
 */
public record StepDefinition(
        String name,
        StepKind kind,
        @Nullable ClassName executionClass,
        Optional<String> delegatedMethodName,
        @Nullable PipelineTemplateStepExecution remoteExecution,
        Map<String, Object> awaitConfig,
        @Nullable String timeout,
        List<String> idempotencyKeyFields,
        @Nullable String command,
        @Nullable ClassName commandIdGenerator,
        @Nullable String duplicatePolicy,
        Map<String, Object> commandConfig,
        @Nullable String queryId,
        Map<String, Object> queryConfig,
        List<String> queryKeyFields,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint,
        boolean runOnVirtualThreads
) {
    public StepDefinition(
        String name,
        StepKind kind,
        @Nullable ClassName executionClass,
        @Nullable PipelineTemplateStepExecution remoteExecution,
        Map<String, Object> awaitConfig,
        @Nullable String timeout,
        List<String> idempotencyKeyFields,
        @Nullable String command,
        @Nullable ClassName commandIdGenerator,
        @Nullable String duplicatePolicy,
        Map<String, Object> commandConfig,
        @Nullable String queryId,
        Map<String, Object> queryConfig,
        List<String> queryKeyFields,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint,
        boolean runOnVirtualThreads
    ) {
        this(
            name,
            kind,
            executionClass,
            Optional.empty(),
            remoteExecution,
            awaitConfig,
            timeout,
            idempotencyKeyFields,
            command,
            commandIdGenerator,
            duplicatePolicy,
            commandConfig,
            queryId,
            queryConfig,
            queryKeyFields,
            inboundMapper,
            outboundMapper,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            runOnVirtualThreads);
    }

    public StepDefinition(
        String name,
        StepKind kind,
        @Nullable ClassName executionClass,
        @Nullable PipelineTemplateStepExecution remoteExecution,
        Map<String, Object> awaitConfig,
        @Nullable String timeout,
        List<String> idempotencyKeyFields,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
    ) {
        this(
            name,
            kind,
            executionClass,
            Optional.empty(),
            remoteExecution,
            awaitConfig,
            timeout,
            idempotencyKeyFields,
            null,
            null,
            null,
            Map.of(),
            null,
            Map.of(),
            List.of(),
            inboundMapper,
            outboundMapper,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            false);
    }

    public StepDefinition(
        String name,
        StepKind kind,
        @Nullable ClassName executionClass,
        @Nullable PipelineTemplateStepExecution remoteExecution,
        Map<?, ?> awaitConfig,
        @Nullable String timeout,
        List<?> idempotencyKeyFields,
        @Nullable String queryId,
        Map<?, ?> queryConfig,
        List<?> queryKeyFields,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint,
        boolean runOnVirtualThreads
    ) {
        this(
            name,
            kind,
            executionClass,
            Optional.empty(),
            remoteExecution,
            copyObjectMap(awaitConfig),
            timeout,
            copyStringList(idempotencyKeyFields),
            null,
            null,
            null,
            Map.of(),
            queryId,
            copyObjectMap(queryConfig),
            copyStringList(queryKeyFields),
            inboundMapper,
            outboundMapper,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            runOnVirtualThreads);
    }

    public StepDefinition(
        String name,
        StepKind kind,
        ClassName executionClass,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
    ) {
        this(
            name,
            requireNonRemoteKind(kind),
            executionClass,
            Optional.empty(),
            null,
            Map.of(),
            null,
            List.of(),
            null,
            null,
            null,
            Map.of(),
            null,
            Map.of(),
            List.of(),
            null,
            null,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            false);
    }

    public StepDefinition(
        String name,
        StepKind kind,
        ClassName executionClass,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
    ) {
        this(
            name,
            requireNonRemoteKind(kind),
            executionClass,
            Optional.empty(),
            null,
            Map.of(),
            null,
            List.of(),
            null,
            null,
            null,
            Map.of(),
            null,
            Map.of(),
            List.of(),
            inboundMapper,
            outboundMapper,
            null,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            false);
    }

    public StepDefinition(
        String name,
        StepKind kind,
        ClassName executionClass,
        Optional<String> delegatedMethodName,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint,
        boolean runOnVirtualThreads
    ) {
        this(
            name,
            requireNonRemoteKind(kind),
            executionClass,
            delegatedMethodName,
            null,
            Map.of(),
            null,
            List.of(),
            null,
            null,
            null,
            Map.of(),
            null,
            Map.of(),
            List.of(),
            inboundMapper,
            outboundMapper,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            runOnVirtualThreads);
    }

    public StepDefinition(
        String name,
        StepKind kind,
        ClassName executionClass,
        @Nullable ClassName inboundMapper,
        @Nullable ClassName outboundMapper,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
    ) {
        this(
            name,
            requireNonRemoteKind(kind),
            executionClass,
            Optional.empty(),
            null,
            Map.of(),
            null,
            List.of(),
            null,
            null,
            null,
            Map.of(),
            null,
            Map.of(),
            List.of(),
            inboundMapper,
            outboundMapper,
            externalMapper,
            mapperFallback,
            inputType,
            outputType,
            streamingShapeHint,
            false);
    }

    public StepDefinition {
        Objects.requireNonNull(name, "Name cannot be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Name cannot be blank");
        }
        Objects.requireNonNull(kind, "Kind cannot be null");
        if (runOnVirtualThreads && kind != StepKind.INTERNAL) {
            throw new IllegalArgumentException("runOnVirtualThreads is valid only for INTERNAL steps");
        }
        if (executionClass != null && remoteExecution != null) {
            throw new IllegalArgumentException("executionClass and remoteExecution are mutually exclusive");
        }
        delegatedMethodName = normalizeOptionalString(delegatedMethodName);
        if (kind == StepKind.REMOTE) {
            Objects.requireNonNull(remoteExecution, "Remote execution cannot be null for REMOTE steps");
        } else if (kind == StepKind.AWAIT || kind == StepKind.COMMAND || kind == StepKind.QUERY) {
            if (executionClass != null || remoteExecution != null) {
                throw new IllegalArgumentException(kind + " steps cannot declare executionClass or remoteExecution");
            }
            Objects.requireNonNull(inputType, "Input type cannot be null for " + kind + " steps");
            Objects.requireNonNull(outputType, "Output type cannot be null for " + kind + " steps");
            if (kind == StepKind.COMMAND) {
                if (command == null || command.isBlank()) {
                    throw new IllegalArgumentException("Command cannot be blank for COMMAND steps");
                }
                Objects.requireNonNull(commandIdGenerator, "Command id generator cannot be null for COMMAND steps");
            }
            if (kind == StepKind.QUERY) {
                Objects.requireNonNull(queryId, "Query id cannot be null for QUERY steps");
                if (queryId.isBlank()) {
                    throw new IllegalArgumentException("Query id cannot be blank for QUERY steps");
                }
            }
        } else {
            Objects.requireNonNull(executionClass, "Execution class cannot be null");
        }
        mapperFallback = mapperFallback == null ? MapperFallbackMode.NONE : mapperFallback;
        awaitConfig = awaitConfig == null ? Map.of() : Map.copyOf(awaitConfig);
        idempotencyKeyFields = idempotencyKeyFields == null ? List.of() : List.copyOf(idempotencyKeyFields);
        commandConfig = commandConfig == null ? Map.of() : Map.copyOf(commandConfig);
        queryConfig = queryConfig == null ? Map.of() : Map.copyOf(queryConfig);
        queryKeyFields = queryKeyFields == null ? List.of() : List.copyOf(queryKeyFields);
    }

    private static StepKind requireNonRemoteKind(StepKind kind) {
        if (kind == StepKind.REMOTE || kind == StepKind.AWAIT || kind == StepKind.COMMAND || kind == StepKind.QUERY) {
            throw new IllegalArgumentException("Convenience constructor cannot be used for " + kind);
        }
        return kind;
    }

    private static Optional<String> normalizeOptionalString(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String normalized = value.get().trim();
        return normalized.isBlank() ? Optional.empty() : Optional.of(normalized);
    }

    private static Map<String, Object> copyObjectMap(Map<?, ?> values) {
        if (values == null) {
            return Map.of();
        }
        java.util.LinkedHashMap<String, Object> copy = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            copy.put(entry.getKey() == null ? null : entry.getKey().toString(), entry.getValue());
        }
        return Map.copyOf(copy);
    }

    private static List<String> copyStringList(List<?> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
            .map(value -> value == null ? null : value.toString())
            .toList();
    }
}
