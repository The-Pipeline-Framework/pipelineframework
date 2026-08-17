package org.pipelineframework.connector.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.pipelineframework.protocol.ProtocolTypeContributor;

class LlmProtocolTypeContributorTest {
    @Test
    void contributesPortableAgentCallThroughTheProtocolTypeSeam() {
        var contributions = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .flatMap(contributor -> contributor.protocolTypes().stream())
            .filter(type -> type.identity().qualifiedName().equals("tpf.llm.AgentCall"))
            .toList();

        assertEquals(1, contributions.size());
        var record = (org.pipelineframework.config.template.PipelineTemplateTypeDefinition.RecordType)
            contributions.getFirst().definition();
        assertEquals(java.util.List.of("binding", "operation", "argumentsJson"),
            record.fields().stream().map(org.pipelineframework.config.template.PipelineTemplateTypeDefinition.Field::name).toList());
    }
}
