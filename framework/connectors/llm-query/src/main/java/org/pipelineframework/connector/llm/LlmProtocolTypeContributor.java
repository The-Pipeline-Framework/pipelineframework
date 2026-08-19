package org.pipelineframework.connector.llm;

import java.util.Collection;
import java.util.List;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Contributes the portable proposal payload owned by the LLM Query connector. */
public final class LlmProtocolTypeContributor implements ProtocolTypeContributor {
    public static final ProtocolTypeIdentity AGENT_CALL =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.llm"), "AgentCall");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        PipelineTemplateTypeReference string = new PipelineTemplateTypeReference.Scalar("string");
        return List.of(new ProtocolTypeDescriptor(
            AGENT_CALL,
            new PipelineTemplateTypeDefinition.RecordType("AgentCall", List.of(
                new PipelineTemplateTypeDefinition.Field("binding", string),
                new PipelineTemplateTypeDefinition.Field("operation", string),
                new PipelineTemplateTypeDefinition.Field("argumentsJson", string)))));
    }
}
