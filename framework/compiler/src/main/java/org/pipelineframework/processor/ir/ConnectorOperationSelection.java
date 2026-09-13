package org.pipelineframework.processor.ir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.processor.composition.PipelineReference;

/**
 * Compiler-owned, immutable selection of one application connector operation.
 *
 * <p>This is the normalized semantic state used by generated operation-first Query and Command
 * steps. It deliberately contains no Block-specific execution concept.</p>
 */
public record ConnectorOperationSelection(
    PipelineReference definition,
    String authoredStepName,
    String runtimeStepId,
    ConnectorBindingName binding,
    ConnectorOperationIdentity operation,
    int providerMajorVersion,
    Map<String, Object> operationConfiguration,
    Optional<QuerySelection> query,
    Optional<CommandSelection> command
) {
    public ConnectorOperationSelection {
        definition = Objects.requireNonNull(definition, "operation definition must not be null");
        authoredStepName = requireText(authoredStepName, "authored step name");
        runtimeStepId = requireText(runtimeStepId, "runtime step ID");
        binding = Objects.requireNonNull(binding, "connector binding must not be null");
        operation = Objects.requireNonNull(operation, "connector operation must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("provider major version must be positive");
        }
        operationConfiguration = immutableMap(operationConfiguration);
        query = Objects.requireNonNull(query, "query selection must not be null");
        command = Objects.requireNonNull(command, "command selection must not be null");
        if (ConnectorOperationKind.QUERY.equals(operation.kind()) && (query.isEmpty() || command.isPresent())) {
            throw new IllegalArgumentException("Query operation selection requires only Query semantics");
        }
        if (ConnectorOperationKind.COMMAND.equals(operation.kind()) && (command.isEmpty() || query.isPresent())) {
            throw new IllegalArgumentException("Command operation selection requires only Command semantics");
        }
        if (!ConnectorOperationKind.QUERY.equals(operation.kind())
            && !ConnectorOperationKind.COMMAND.equals(operation.kind())) {
            throw new IllegalArgumentException("connector operation selection requires Query or Command kind");
        }
    }

    public static ConnectorOperationSelection query(
        String authoredStepName,
        ConnectorBindingName binding,
        ConnectorOperationIdentity operation,
        int providerMajorVersion,
        Map<String, Object> operationConfiguration,
        QuerySelection query
    ) {
        return new ConnectorOperationSelection(
            new PipelineReference("$root"), authoredStepName, authoredStepName, binding, operation,
            providerMajorVersion, operationConfiguration, Optional.of(query), Optional.empty());
    }

    public static ConnectorOperationSelection command(
        String authoredStepName,
        ConnectorBindingName binding,
        ConnectorOperationIdentity operation,
        int providerMajorVersion,
        Map<String, Object> operationConfiguration,
        CommandSelection command
    ) {
        return new ConnectorOperationSelection(
            new PipelineReference("$root"), authoredStepName, authoredStepName, binding, operation,
            providerMajorVersion, operationConfiguration, Optional.empty(), Optional.of(command));
    }

    /** Binds definition and generated step identity after named-pipeline model extraction. */
    public ConnectorOperationSelection withLinkedIdentity(
        PipelineReference linkedDefinition,
        String linkedRuntimeStepId
    ) {
        return new ConnectorOperationSelection(
            linkedDefinition, authoredStepName, linkedRuntimeStepId, binding, operation,
            providerMajorVersion, operationConfiguration, query, command);
    }

    public record QuerySelection(
        QueryOperationCardinality cardinality,
        QueryCapabilities capabilities,
        Optional<Duration> negativeCacheTtl,
        Map<String, Object> capture,
        List<String> keyFields
    ) {
        public QuerySelection {
            cardinality = Objects.requireNonNull(cardinality, "query cardinality must not be null");
            capabilities = Objects.requireNonNull(capabilities, "query capabilities must not be null");
            negativeCacheTtl = Objects.requireNonNull(negativeCacheTtl, "negative cache TTL must not be null");
            capture = immutableMap(capture);
            keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        }
    }

    public record CommandSelection(
        ClassName commandIdGenerator,
        CommandDuplicatePolicy duplicatePolicy,
        CommandPolicy policy
    ) {
        public CommandSelection {
            commandIdGenerator = Objects.requireNonNull(commandIdGenerator, "command ID generator must not be null");
            duplicatePolicy = Objects.requireNonNull(duplicatePolicy, "duplicate policy must not be null");
            policy = Objects.requireNonNull(policy, "command policy must not be null");
        }
    }

    private static Map<String, Object> immutableMap(Map<String, ?> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(requireText(key, "configuration field"), immutableValue(value)));
        return Collections.unmodifiableMap(copy);
    }

    private static Object immutableValue(Object value) {
        Objects.requireNonNull(value, "configuration value must not be null");
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, child) -> copy.put(String.valueOf(key), immutableValue(child)));
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>();
            list.forEach(child -> copy.add(immutableValue(child)));
            return List.copyOf(copy);
        }
        return value;
    }

    private static String requireText(String value, String subject) {
        Objects.requireNonNull(value, subject + " must not be null");
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(subject + " must not be blank");
        }
        return normalized;
    }
}
