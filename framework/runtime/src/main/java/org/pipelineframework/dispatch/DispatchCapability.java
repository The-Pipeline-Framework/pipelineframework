package org.pipelineframework.dispatch;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.QueryCapabilities;

/** Compiler-landed trusted facts for one explicitly exposed capability. */
public record DispatchCapability(
    BoundOperationReference reference,
    ConnectorOperationIdentity identity,
    int providerMajorVersion,
    String inputType,
    Class<?> inputClass,
    String outputType,
    Class<?> outputClass,
    Map<String, Object> configuration,
    Optional<QueryCapabilities> queryCapabilities,
    Optional<CommandConfiguration> commandConfiguration
) {
    public DispatchCapability {
        reference = Objects.requireNonNull(reference, "bound operation reference must not be null");
        identity = Objects.requireNonNull(identity, "operation identity must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("provider major version must be positive");
        }
        inputType = requireText(inputType, "input type");
        inputClass = Objects.requireNonNull(inputClass, "input class must not be null");
        outputType = requireText(outputType, "output type");
        outputClass = Objects.requireNonNull(outputClass, "output class must not be null");
        configuration = Map.copyOf(Objects.requireNonNull(configuration, "operation configuration must not be null"));
        queryCapabilities = Objects.requireNonNull(queryCapabilities, "query capabilities must not be null");
        commandConfiguration = Objects.requireNonNull(commandConfiguration, "command configuration must not be null");
        if (identity.kind().equals(org.pipelineframework.connector.ConnectorOperationKind.QUERY)
            != queryCapabilities.isPresent()) {
            throw new IllegalArgumentException("Query dispatch capability must declare Query capabilities only");
        }
        if (identity.kind().equals(org.pipelineframework.connector.ConnectorOperationKind.COMMAND)
            != commandConfiguration.isPresent()) {
            throw new IllegalArgumentException("Command dispatch capability must declare Command configuration only");
        }
    }

    private static String requireText(String value, String subject) {
        String normalized = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(subject + " must not be blank");
        }
        return normalized;
    }

    public record CommandConfiguration(
        String commandIdGenerator,
        CommandDuplicatePolicy duplicatePolicy,
        CommandPolicy policy
    ) {
        public CommandConfiguration {
            commandIdGenerator = requireText(commandIdGenerator, "command ID generator");
            duplicatePolicy = Objects.requireNonNull(duplicatePolicy, "command duplicate policy must not be null");
            policy = Objects.requireNonNull(policy, "command policy must not be null");
        }
    }
}
