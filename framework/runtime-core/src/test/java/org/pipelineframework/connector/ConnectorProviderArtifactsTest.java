package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.config.template.PipelineTemplateWrapperConstraints;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

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
        assertTrue(json.contains("\"schemaVersion\":5"));
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

    @Test
    void roundTripsCanonicalProtocolFieldSemantics() {
        ConnectorProviderId providerId = ConnectorProviderId.of("acme.protocol");
        ProtocolTypeDescriptor type = new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(providerId, "Request"),
            new PipelineTemplateTypeDefinition.RecordType("Request", List.of(
                new PipelineTemplateTypeDefinition.Field(
                    "note", new PipelineTemplateTypeReference.Scalar("string"), false,
                    PipelineFieldPresence.OPTIONAL, PipelineFieldNullability.NULLABLE),
                new PipelineTemplateTypeDefinition.Field(
                    "tags", new PipelineTemplateTypeReference.Scalar("string"), true))));
        ConnectorProviderManifest manifest = new ConnectorProviderManifest(
            ConnectorProviderManifest.CURRENT_SCHEMA_VERSION,
            List.of(new ConnectorProviderArtifactDescriptor(
                new ConnectorProviderDescriptor(providerId, new ConnectorProviderVersion(1, 0)),
                List.of(), List.of(type))));

        String json = ConnectorProviderArtifacts.json(manifest);
        ConnectorProviderManifest parsed = ConnectorProviderManifestReader.read(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

        assertEquals(manifest, parsed);
        assertTrue(json.contains("\"presence\":\"OPTIONAL\""));
        assertTrue(json.contains("\"nullability\":\"NULLABLE\""));
        assertTrue(json.contains("\"repeated\":true"));
    }

    @Test
    void rejectsProtocolFieldModifiersBeforeManifestSchemaFive() {
        String json = """
            {"schemaVersion":4,"providers":[{"id":"acme.protocol","version":{"major":1,"minor":0},
            "operations":[],"protocolTypes":[{"name":"Request","fields":[
            {"name":"note","type":"string","presence":"OPTIONAL"}]}]}]}
            """;

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));

        assertTrue(failure.getMessage().contains("schema versions before 5"));
    }

    @Test
    void roundTripsSignedAndFractionalCanonicalNumericConstraints() {
        ConnectorProviderId providerId = ConnectorProviderId.of("acme.protocol");
        ProtocolTypeDescriptor type = new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(providerId, "Temperature"),
            new PipelineTemplateTypeDefinition.WrapperType(
                "Temperature",
                new PipelineTemplateTypeReference.Scalar("decimal"),
                new PipelineTemplateWrapperConstraints(
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.of(new BigDecimal("-273.15")), Optional.empty(),
                    Optional.of(new BigDecimal("1.0E+6")), Optional.empty())));
        ConnectorProviderManifest manifest = new ConnectorProviderManifest(
            ConnectorProviderManifest.CURRENT_SCHEMA_VERSION,
            List.of(new ConnectorProviderArtifactDescriptor(
                new ConnectorProviderDescriptor(providerId, new ConnectorProviderVersion(1, 0)),
                List.of(), List.of(type))));

        String json = ConnectorProviderArtifacts.json(manifest);
        ConnectorProviderManifest parsed = ConnectorProviderManifestReader.read(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

        assertEquals(manifest, parsed);
        assertTrue(json.contains("\"minimum\":-273.15"));
    }

    @Test
    void projectsStreamingQueryCardinalityWithoutUnaryCacheCapabilities() {
        ConnectorOperationDescriptor descriptor = ConnectorDescriptors.operation(new StringRowsQuery());

        assertEquals(ConnectorOperationKind.QUERY, descriptor.kind());
        assertEquals(Optional.of(QueryOperationCardinality.ONE_TO_MANY), descriptor.queryCardinality());
        assertTrue(descriptor.queryCapabilities().isEmpty());
        assertEquals("string", descriptor.typeContract().orElseThrow().inputType());
        assertEquals(Optional.of("integer"), descriptor.typeContract().orElseThrow().outputType());
    }

    @Test
    void schemaLessStreamingQueryRejectsAnArbitraryConfigurationType() {
        ConnectorConfigurationException failure = assertThrows(
            ConnectorConfigurationException.class,
            () -> new TypedRowsQuery().query(
                "input",
                ConnectorConfigurationDocument.empty(),
                Integer.class,
                ConnectorExecutionContext.empty()));

        assertTrue(failure.getMessage().contains("must declare a configuration schema for"));
        assertTrue(failure.getMessage().contains(TypedConfiguration.class.getName()));
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

    private static final class StringRowsQuery
        implements StreamingQueryOperation<String, ConnectorConfigurationDocument, Integer> {
        @Override
        public String id() {
            return "string.rows";
        }

        @Override
        public QueryStream<Integer> query(
            QueryInvocation<String, ConnectorConfigurationDocument, Integer> invocation
        ) {
            return new QueryStream<>(subscriber -> subscriber.onSubscribe(new java.util.concurrent.Flow.Subscription() {
                @Override
                public void request(long count) {
                    subscriber.onComplete();
                }

                @Override
                public void cancel() {
                }
            }), CompletableFuture.completedFuture(null));
        }
    }

    private record TypedConfiguration(String value) {
    }

    private static final class TypedRowsQuery
        implements StreamingQueryOperation<String, TypedConfiguration, Integer> {
        @Override
        public String id() {
            return "typed.rows";
        }

        @Override
        public QueryStream<Integer> query(QueryInvocation<String, TypedConfiguration, Integer> invocation) {
            throw new AssertionError("invalid configuration must fail before provider invocation");
        }
    }
}
