package org.pipelineframework.connector.vector;

import java.util.Collection;
import java.util.List;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Contributes the portable vector upsert and search vocabulary. */
public final class VectorProtocolTypeContributor implements ProtocolTypeContributor {
    private static final ConnectorProviderId NAMESPACE = ConnectorProviderId.of("tpf.vector");
    public static final ProtocolTypeIdentity UPSERT_REQUEST = new ProtocolTypeIdentity(NAMESPACE, "VectorUpsertRequest");
    public static final ProtocolTypeIdentity UPSERT_RESULT = new ProtocolTypeIdentity(NAMESPACE, "VectorUpsertResult");
    public static final ProtocolTypeIdentity SEARCH_REQUEST = new ProtocolTypeIdentity(NAMESPACE, "VectorSearchRequest");
    public static final ProtocolTypeIdentity MATCH = new ProtocolTypeIdentity(NAMESPACE, "VectorMatch");
    public static final ProtocolTypeIdentity SEARCH_RESULT = new ProtocolTypeIdentity(NAMESPACE, "VectorSearchResult");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        var string = new PipelineTemplateTypeReference.Scalar("string");
        var int32 = new PipelineTemplateTypeReference.Scalar("int32");
        var float32 = new PipelineTemplateTypeReference.Scalar("float32");
        return List.of(
            record(UPSERT_REQUEST, field("itemId", string), field("content", string), repeated("values", float32)),
            record(UPSERT_RESULT, field("itemId", string)),
            record(SEARCH_REQUEST, field("queryId", string), field("queryText", string), repeated("values", float32), field("limit", int32)),
            record(MATCH, field("itemId", string), field("content", string), field("score", float32)),
            record(SEARCH_RESULT, field("queryId", string), field("queryText", string),
                repeated("matches", new PipelineTemplateTypeReference.Contributed(MATCH.qualifiedName()))));
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
}
