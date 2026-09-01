package org.pipelineframework.connector.embedding;

import java.util.Collection;
import java.util.List;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Contributes the portable embedding request and result vocabulary. */
public final class EmbeddingProtocolTypeContributor implements ProtocolTypeContributor {
    public static final ProtocolTypeIdentity REQUEST =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.embedding"), "EmbeddingRequest");
    public static final ProtocolTypeIdentity RESULT =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.embedding"), "EmbeddingResult");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        var string = new PipelineTemplateTypeReference.Scalar("string");
        var float32 = new PipelineTemplateTypeReference.Scalar("float32");
        return List.of(
            new ProtocolTypeDescriptor(REQUEST, new PipelineTemplateTypeDefinition.RecordType("EmbeddingRequest", List.of(
                new PipelineTemplateTypeDefinition.Field("itemId", string),
                new PipelineTemplateTypeDefinition.Field("text", string)))),
            new ProtocolTypeDescriptor(RESULT, new PipelineTemplateTypeDefinition.RecordType("EmbeddingResult", List.of(
                new PipelineTemplateTypeDefinition.Field("itemId", string),
                new PipelineTemplateTypeDefinition.Field("text", string),
                new PipelineTemplateTypeDefinition.Field("values", float32, true)))));
    }
}
