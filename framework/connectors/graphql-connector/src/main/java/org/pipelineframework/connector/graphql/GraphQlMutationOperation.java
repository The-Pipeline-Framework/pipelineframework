package org.pipelineframework.connector.graphql;

import java.util.Optional;

import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.ConnectorConfigSchema;

/** Provider-neutral GraphQL mutation Command operation. */
public interface GraphQlMutationOperation
    extends CommandOperation<GraphQlMutationRequest, GraphQlOperationConfiguration, GraphQlResponse> {
    ConnectorConfigSchema<GraphQlOperationConfiguration> CONFIGURATION = ConnectorConfigSchema.record(
        GraphQlOperationConfiguration.class, "tpf.graphql.execute.mutation", 1);

    @Override
    default String id() {
        return "execute.mutation";
    }

    @Override
    default Optional<ConnectorConfigSchema<GraphQlOperationConfiguration>> configurationSchema() {
        return Optional.of(CONFIGURATION);
    }
}
