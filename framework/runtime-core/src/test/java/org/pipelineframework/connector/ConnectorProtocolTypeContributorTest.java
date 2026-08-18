package org.pipelineframework.connector;

import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.protocol.ProtocolTypeContributor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ConnectorProtocolTypeContributorTest {
    @Test
    void contributesUnionBackedOperationObservationWithoutNullablePayloadFields() {
        var types = ServiceLoader.load(ProtocolTypeContributor.class).stream()
            .map(ServiceLoader.Provider::get)
            .flatMap(contributor -> contributor.protocolTypes().stream())
            .filter(type -> type.identity().namespace().equals(ConnectorProviderId.of("tpf.connector")))
            .toList();

        assertEquals(3, types.size());
        var observation = types.stream()
            .filter(type -> type.identity().equals(ConnectorProtocolTypeContributor.OPERATION_OBSERVATION))
            .findFirst().orElseThrow();
        var union = assertInstanceOf(PipelineTemplateTypeDefinition.UnionType.class, observation.definition());
        assertEquals(java.util.Set.of("result", "empty"), union.variants().keySet());
    }
}
