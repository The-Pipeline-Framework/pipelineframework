package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class ConnectorProviderArtifactsTest {
    @Test
    void escapesEveryJsonControlCharacterInAuthorMetadata() {
        ConnectorConfigSchemaDescriptor schema = new ConnectorConfigSchemaDescriptor(
            "acme.control",
            1,
            List.of(new ConnectorConfigFieldDescriptor(
                "mode", ConnectorConfigValueType.ENUM, true, List.of("line\nfeed", "tab\tvalue", "unit\u0001separator"))));
        ConnectorProviderManifest manifest = new ConnectorProviderManifest(
            ConnectorProviderManifest.CURRENT_SCHEMA_VERSION,
            List.of(new ConnectorProviderArtifactDescriptor(
                new ConnectorProviderDescriptor(ConnectorProviderId.of("acme.control"), new ConnectorProviderVersion(1, 0)),
                List.of(new ConnectorOperationDescriptor(
                    "read", ConnectorOperationKind.QUERY, 1, Optional.of(schema))))));

        String json = ConnectorProviderArtifacts.json(manifest);
        ConnectorProviderManifest parsed = ConnectorProviderManifestReader.read(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

        assertTrue(json.contains("line\\nfeed"));
        assertTrue(json.contains("tab\\tvalue"));
        assertTrue(json.contains("unit\\u0001separator"));
        assertTrue(json.contains("\"schemaVersion\":3"));
        assertTrue(!json.contains("executionCapabilities"));
        assertEquals(manifest, parsed);
    }

    @Test
    void resolvesTypeContractsThroughBlockingFamilySpecialization() {
        ConnectorOperationDescriptor descriptor = ConnectorDescriptors.operation(new BlockingStringQuery());

        ConnectorOperationTypeContract contract = descriptor.typeContract().orElseThrow();
        assertEquals("string", contract.inputType());
        assertEquals(Optional.of("integer"), contract.outputType());
    }

    private static final class BlockingStringQuery
        implements BlockingQueryOperation<String, ConnectorConfigurationDocument, Integer> {
        @Override
        public String id() {
            return "blocking.string";
        }

        @Override
        public CompletionStage<QueryOutcome<Integer>> query(
            QueryInvocation<String, ConnectorConfigurationDocument, Integer> invocation
        ) {
            return CompletableFuture.completedFuture(new QueryOutcome.Found<>(1));
        }
    }
}
