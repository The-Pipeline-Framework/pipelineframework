package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;
import java.util.Optional;

/**
 * Command-family operation contract. Command outcome semantics are defined by the command runtime work.
 */
public interface CommandOperation<I, C, O> extends ConnectorOperation {
    default CommandCapabilities capabilities() {
        return CommandCapabilities.conservative();
    }

    default Optional<ConnectorConfigSchema<C>> configurationSchema() {
        return Optional.empty();
    }

    CompletionStage<CommandOutcome<O>> dispatch(CommandInvocation<I, C> invocation);

    default CompletionStage<CommandOutcome<O>> dispatch(
        I input,
        ConnectorConfigurationDocument configuration,
        ConnectorExecutionContext executionContext
    ) {
        ConnectorConfigSchema<C> schema = configurationSchema().orElseThrow(() -> new ConnectorConfigurationException(
            "command operation " + id() + " does not declare a configuration schema"));
        C boundConfiguration = ConnectorConfigurationBinder.bind(schema, configuration, "command operation " + id());
        return dispatch(new CommandInvocation<>(input, boundConfiguration, executionContext));
    }
}
