package org.pipelineframework.connector.llm;

import java.util.Collection;
import java.util.List;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Contributes the portable decision payloads owned by the LLM Query connector. */
public final class LlmProtocolTypeContributor implements ProtocolTypeContributor {
    public static final ProtocolTypeIdentity AGENT_CALL =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.llm"), "AgentCall");
    public static final ProtocolTypeIdentity ASK_USER =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.llm"), "AskUser");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        PipelineTemplateTypeReference string = new PipelineTemplateTypeReference.Scalar("string");
        return List.of(
            new ProtocolTypeDescriptor(
                AGENT_CALL,
                new PipelineTemplateTypeDefinition.RecordType("AgentCall", List.of(
                    new PipelineTemplateTypeDefinition.Field("binding", string),
                    new PipelineTemplateTypeDefinition.Field("operation", string),
                    new PipelineTemplateTypeDefinition.Field("argumentsJson", string)))),
            new ProtocolTypeDescriptor(
                ASK_USER,
                new PipelineTemplateTypeDefinition.RecordType("AskUser", List.of(
                    new PipelineTemplateTypeDefinition.Field("prompt", string),
                    new PipelineTemplateTypeDefinition.Field("choices", string, true)))));
    }
}
