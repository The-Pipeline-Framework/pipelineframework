package org.pipelineframework.connector.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.pipelineframework.protocol.ProtocolTypeContributor;

class LlmProtocolTypeContributorTest {
    @Test
    void contributesPortableLlmDecisionTypesThroughTheProtocolTypeSeam() {
        var contributions = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .flatMap(contributor -> contributor.protocolTypes().stream())
            .toList();

        var agentCall = contributions.stream()
            .filter(type -> type.identity().qualifiedName().equals("tpf.llm.AgentCall"))
            .findFirst()
            .orElseThrow();
        var record = (org.pipelineframework.config.template.PipelineTemplateTypeDefinition.RecordType)
            agentCall.definition();
        assertEquals(java.util.List.of("binding", "operation", "argumentsJson"),
            record.fields().stream().map(org.pipelineframework.config.template.PipelineTemplateTypeDefinition.Field::name).toList());

        var askUser = contributions.stream()
            .filter(type -> type.identity().qualifiedName().equals("tpf.llm.AskUser"))
            .findFirst()
            .orElseThrow();
        var askUserRecord = (org.pipelineframework.config.template.PipelineTemplateTypeDefinition.RecordType)
            askUser.definition();
        assertEquals(java.util.List.of("prompt", "choices"), askUserRecord.fields().stream()
            .map(org.pipelineframework.config.template.PipelineTemplateTypeDefinition.Field::name).toList());
        assertEquals(false, askUserRecord.fields().getFirst().repeated());
        assertEquals(true, askUserRecord.fields().getLast().repeated());
    }
}
