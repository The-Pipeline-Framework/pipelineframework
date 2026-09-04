package org.pipelineframework.connector.graphql;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.config.template.PipelineTemplateWrapperConstraints;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Contributes the provider-neutral GraphQL request, response, error and result vocabulary. */
public final class GraphQlProtocolTypeContributor implements ProtocolTypeContributor {
    private static final ConnectorProviderId NAMESPACE = ConnectorProviderId.of("tpf.graphql");
    public static final ProtocolTypeIdentity VARIABLES_JSON = identity("GraphQlVariablesJson");
    public static final ProtocolTypeIdentity DATA_JSON = identity("GraphQlDataJson");
    public static final ProtocolTypeIdentity QUERY_REQUEST = identity("GraphQlQueryRequest");
    public static final ProtocolTypeIdentity MUTATION_REQUEST = identity("GraphQlMutationRequest");
    public static final ProtocolTypeIdentity ERROR = identity("GraphQlError");
    public static final ProtocolTypeIdentity RESPONSE = identity("GraphQlResponse");
    public static final ProtocolTypeIdentity RESULT = identity("GraphQlResult");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        var string = new PipelineTemplateTypeReference.Scalar("string");
        var variables = contributed(VARIABLES_JSON);
        var data = contributed(DATA_JSON);
        var error = contributed(ERROR);
        return List.of(
            wrapper(VARIABLES_JSON, string),
            wrapper(DATA_JSON, string),
            record(QUERY_REQUEST, field("operationKey", string), field("variablesJson", variables)),
            record(MUTATION_REQUEST, field("operationKey", string), field("effectKey", string),
                field("variablesJson", variables)),
            record(ERROR, field("code", string), repeated("path", string), field("message", string)),
            record(RESPONSE, optional("data", data), repeated("errors", error)),
            record(RESULT, optional("data", data), repeated("errors", error)));
    }

    private static ProtocolTypeIdentity identity(String name) {
        return new ProtocolTypeIdentity(NAMESPACE, name);
    }

    private static PipelineTemplateTypeReference contributed(ProtocolTypeIdentity identity) {
        return new PipelineTemplateTypeReference.Contributed(identity.qualifiedName());
    }

    private static ProtocolTypeDescriptor wrapper(
        ProtocolTypeIdentity identity,
        PipelineTemplateTypeReference.Scalar scalar
    ) {
        return new ProtocolTypeDescriptor(identity, new PipelineTemplateTypeDefinition.WrapperType(
            identity.typeName(), scalar, new PipelineTemplateWrapperConstraints(
                Optional.of(2), Optional.of(GraphQlJsonObjects.MAX_JSON_LENGTH), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())));
    }

    private static ProtocolTypeDescriptor record(
        ProtocolTypeIdentity identity,
        PipelineTemplateTypeDefinition.Field... fields
    ) {
        return new ProtocolTypeDescriptor(identity,
            new PipelineTemplateTypeDefinition.RecordType(identity.typeName(), List.of(fields)));
    }

    private static PipelineTemplateTypeDefinition.Field field(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(name, type);
    }

    private static PipelineTemplateTypeDefinition.Field repeated(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(name, type, true);
    }

    private static PipelineTemplateTypeDefinition.Field optional(String name, PipelineTemplateTypeReference type) {
        return new PipelineTemplateTypeDefinition.Field(
            name, type, false, PipelineFieldPresence.OPTIONAL, PipelineFieldNullability.NON_NULL);
    }
}
