package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/** Projects an immutable callable snapshot from static metadata and an already-authorized identity set. */
public final class CallableOperationSnapshotProjector {
    public CallableOperationSnapshot project(
        ConnectorProviderManifestCatalog metadata,
        CallableOperationAuthorization authorization
    ) {
        Objects.requireNonNull(metadata, "connector provider metadata must not be null");
        Objects.requireNonNull(authorization, "callable operation authorization must not be null");
        List<ConnectorOperationIdentity> identities = authorization.grantedOperations().stream().sorted().toList();
        List<CallableOperationDefinition> definitions = new ArrayList<>();
        for (ConnectorOperationIdentity identity : identities) {
            ConnectorOperationDescriptor descriptor = metadata.operations().get(identity);
            if (descriptor == null) {
                throw new IllegalArgumentException("authorized connector operation has no static metadata: " + identity);
            }
            if (!ConnectorOperationKind.QUERY.equals(identity.kind())
                && !ConnectorOperationKind.COMMAND.equals(identity.kind())) {
                throw new IllegalArgumentException("connector operation kind is not callable in snapshot format v1: "
                    + identity.kind().value());
            }
            ConnectorOperationTypeContract contract = descriptor.typeContract().orElseThrow(() ->
                new IllegalArgumentException("authorized connector operation has no normalized type contract: " + identity));
            definitions.add(new CallableOperationDefinition(
                identity,
                description(identity),
                contract,
                ConnectorOperationKind.COMMAND.equals(identity.kind())
                    ? Optional.of(CallableCommandCapabilities.from(
                        descriptor.commandCapabilities().orElse(CommandCapabilities.conservative())))
                    : Optional.empty(),
                ConnectorOperationKind.QUERY.equals(identity.kind())
                    ? Optional.of(CallableQueryCapabilities.from(
                        descriptor.queryCapabilities().orElse(QueryCapabilities.conservative())))
                    : Optional.empty()));
        }
        return CallableOperationSnapshot.of(definitions);
    }

    private static String description(ConnectorOperationIdentity identity) {
        String operation = identity.operationId().replace('.', ' ').replace('-', ' ').replace('_', ' ');
        return identity.kind().value().toLowerCase(Locale.ROOT) + " operation " + operation;
    }
}
