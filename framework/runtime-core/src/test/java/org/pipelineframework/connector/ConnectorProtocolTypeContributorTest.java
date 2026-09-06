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

        assertEquals(4, types.size());
        var payload = types.stream().filter(type -> type.identity().equals(
            ConnectorProtocolTypeContributor.JSON_PAYLOAD)).findFirst().orElseThrow();
        var record = assertInstanceOf(PipelineTemplateTypeDefinition.RecordType.class, payload.definition());
        assertEquals(java.util.List.of("contentType", "schemaHint", "bodyJson"),
            record.fields().stream().map(PipelineTemplateTypeDefinition.Field::name).toList());
        record.fields().forEach(field -> assertEquals(
            new org.pipelineframework.config.template.PipelineTemplateTypeReference.Scalar("string"), field.type()));
        var observation = types.stream()
            .filter(type -> type.identity().equals(ConnectorProtocolTypeContributor.OPERATION_OBSERVATION))
            .findFirst().orElseThrow();
        var union = assertInstanceOf(PipelineTemplateTypeDefinition.UnionType.class, observation.definition());
        assertEquals(java.util.Set.of("result", "empty"), union.variants().keySet());
    }
}
