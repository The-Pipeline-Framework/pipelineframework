package org.pipelineframework.command;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperationIdentity;

/**
 * Sanitized durable projection of a native command outcome.
 */
public record CommandOutcomeSnapshot(
    ConnectorOperationIdentity operationIdentity,
    int providerMajorVersion,
    ConnectorConfigurationSnapshot configuration,
    CommandEffectStatus outcomeStatus,
    String outcomeCode,
    Set<String> flags,
    CommandMachineConfirmation machineConfirmation,
    boolean userConfirmed,
    List<CommandReference> references
) {
    public CommandOutcomeSnapshot {
        operationIdentity = Objects.requireNonNull(operationIdentity, "operation identity must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("command provider major version must be positive");
        }
        configuration = Objects.requireNonNull(configuration, "command configuration snapshot must not be null");
        outcomeStatus = Objects.requireNonNull(outcomeStatus, "command outcome status must not be null");
        if (outcomeCode == null || outcomeCode.isBlank()) {
            throw new IllegalArgumentException("command outcome code must not be blank");
        }
        flags = Set.copyOf(Objects.requireNonNull(flags, "command outcome flags must not be null"));
        machineConfirmation = Objects.requireNonNull(machineConfirmation, "machine confirmation must not be null");
        references = List.copyOf(Objects.requireNonNull(references, "command references must not be null"));
        if (references.size() > CommandReference.MAX_REFERENCES_PER_OUTCOME) {
            throw new IllegalArgumentException(
                "command references must not contain more than "
                    + CommandReference.MAX_REFERENCES_PER_OUTCOME + " values");
        }
    }
}
