package org.pipelineframework.connector;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorProviderManifestReaderTest {

    @Test
    void readsGeneratedNormalizedTypeContractsAndKeepsVersionOneCompatible() {
        ConnectorProviderManifest current = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1,
            "typeContract":{"input":"string","output":"list<integer>"}}]}]}
            """));
        assertEquals(
            new ConnectorOperationTypeContract("string", java.util.Optional.of("list<integer>")),
            current.providers().getFirst().operations().getFirst().typeContract().orElseThrow());

        ConnectorProviderManifest legacy = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1}]}]}
            """));
        assertTrue(legacy.providers().getFirst().operations().getFirst().typeContract().isEmpty());

        IllegalArgumentException incompatible = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[{"id":"find","kind":"tpf:query","majorVersion":1,
                "typeContract":{"input":"string"}}]}]}
                """)));
        assertTrue(incompatible.getMessage().contains("field absent from schema version 1"));
    }

    @Test
    void readsStaticMetadataWithoutProviderConstruction() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":2},
            "configurationSchema":{"id":"metadata.provider.config","version":1},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1}]}]}
            """));

        ConnectorProviderManifestCatalog catalog = new ConnectorProviderManifestCatalog(List.of(manifest));
        assertEquals("metadata.provider", catalog.providers().getFirst().provider().id().value());
        assertEquals(1, catalog.operations().size());
    }

    @Test
    void readsV2ProtocolTypesAsCanonicalV3Definitions() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[],"protocolTypes":[{"name":"ProtocolCall","fields":[
            {"name":"operation","type":"string"},{"name":"payload","type":"bytes"}]}]}]}
            """));

        ConnectorProviderManifestCatalog catalog = new ConnectorProviderManifestCatalog(List.of(manifest));
        assertEquals(1, catalog.protocolTypes().size());
        assertEquals("metadata.provider.ProtocolCall",
            catalog.protocolTypes().keySet().iterator().next().qualifiedName());
    }

    @Test
    void requiresProtocolUnionVariantsToReferenceContributedTypes() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[],"protocolTypes":[
            {"name":"Success","fields":[{"name":"value","type":"string"}]},
            {"name":"Decision","variants":{"success":"<metadata.provider.Success>"}}]}]}
            """));
        assertInstanceOf(PipelineTemplateTypeDefinition.UnionType.class,
            manifest.providers().getFirst().protocolTypes().get(1).definition());

        IllegalArgumentException scalar = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[{"name":"Decision","variants":{"success":"string"}}]}]}
                """)));
        assertTrue(scalar.getMessage().contains("must reference a contributed protocol type"));
    }

    @Test
    void validatesProtocolWrapperConstraintsWithTheCanonicalScalarRules() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[],"protocolTypes":[
            {"name":"Email","wraps":"string","pattern":"[^@]+@[^@]+","maxLength":128,"format":"email"},
            {"name":"Count","wraps":"int32","minimum":0,"maximum":10}]}]}
            """));
        PipelineTemplateTypeDefinition.WrapperType email = assertInstanceOf(
            PipelineTemplateTypeDefinition.WrapperType.class,
            manifest.providers().getFirst().protocolTypes().getFirst().definition());
        assertEquals(128, email.constraints().maxLength().orElseThrow());
        assertEquals("[^@]+@[^@]+", email.constraints().pattern().orElseThrow());

        IllegalArgumentException wrongScalar = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[{"name":"Flag","wraps":"bool","minimum":0}]}]}
                """)));
        assertTrue(wrongScalar.getMessage().contains("numeric constraints on non-numeric wrapper"));

        IllegalArgumentException emptyInterval = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[
                {"name":"Count","wraps":"int32","minimumExclusive":1,"maximum":1}]}]}
                """)));
        assertTrue(emptyInterval.getMessage().contains("empty numeric constraint interval"));

        IllegalArgumentException unsafePattern = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[
                {"name":"Unsafe","wraps":"string","pattern":"(a+)+","maxLength":128}]}]}
                """)));
        assertTrue(unsafePattern.getMessage().contains("unsafe for runtime model validation"));
    }

    @Test
    void rejectsWrapperConstraintsOnEveryNonWrapperProtocolShape() {
        for (String declaration : List.of(
            "{\"name\":\"Value\",\"fields\":[],\"maxLength\":4}",
            "{\"name\":\"Value\",\"alias\":\"string\",\"maxLength\":4}",
            "{\"name\":\"Value\",\"variants\":{\"value\":\"<metadata.provider.Other>\"},\"maxLength\":4}")) {
            IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> ConnectorProviderManifestReader.read(input("""
                    {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                    "operations":[],"protocolTypes":[%s]}]}
                    """.formatted(declaration))));
            assertTrue(rejected.getMessage().contains("can declare 'maxLength' only beside wraps"));
        }
    }

    @Test
    void rejectsProgrammaticProtocolTypesOutsideTheProviderNamespace() {
        ConnectorProviderArtifactDescriptor artifact = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[]}]}
            """)).providers().getFirst();
        ProtocolTypeDescriptor foreign = new ProtocolTypeDescriptor(
            new ProtocolTypeIdentity(ConnectorProviderId.of("other.provider"), "Value"),
            new PipelineTemplateTypeDefinition.WrapperType(
                "Value", new PipelineTemplateTypeReference.Scalar("string"),
                org.pipelineframework.config.template.PipelineTemplateWrapperConstraints.empty()));

        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
            () -> new ConnectorProviderArtifactDescriptor(artifact.provider(), List.of(), List.of(foreign)));
        assertTrue(rejected.getMessage().contains("must use provider namespace 'metadata.provider'"));
    }

    @Test
    void preservesV1CompatibilityAndRequiresV2ForProtocolTypes() {
        ConnectorProviderManifest v1 = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},"operations":[]}]}
            """));
        assertTrue(v1.providers().getFirst().protocolTypes().isEmpty());

        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[]}]}
                """)));
        assertTrue(rejected.getMessage().contains("schema version 1 cannot declare protocolTypes"));
    }

    @Test
    void rejectsProtocolTypesThatInventAnotherSchemaLanguage() {
        IllegalArgumentException unqualified = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[{"name":"ProtocolCall","fields":[
                {"name":"payload","type":"ApplicationType"}]}]}]}
                """)));
        assertTrue(unqualified.getMessage().contains("supported scalar or qualified contributed type"));

        IllegalArgumentException unknownShape = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":2,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[],"protocolTypes":[{"name":"ProtocolCall","jsonSchema":{}}]}]}
                """)));
        assertTrue(unknownShape.getMessage().contains("unsupported field 'jsonSchema'"));
    }

    @Test
    void rejectsProgrammaticProtocolTypesThatDependOnApplicationTypes() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
            () -> new ProtocolTypeDescriptor(
                new ProtocolTypeIdentity(new ConnectorProviderId("metadata.provider"), "ProtocolCall"),
                new PipelineTemplateTypeDefinition.AliasType(
                    "ProtocolCall", new PipelineTemplateTypeReference.Named("ApplicationType"))));

        assertTrue(rejected.getMessage().contains("closed over v3 scalars and qualified contributed references"));
    }

    @Test
    void rejectsMalformedAndDuplicateStaticMetadataWithActionableDiagnostics() {
        IllegalArgumentException malformed = assertThrows(IllegalArgumentException.class, () -> ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[],"secret":"must-not-appear"}
            """)));
        assertTrue(malformed.getMessage().contains("unsupported field 'secret'"));

        ConnectorProviderManifest first = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"duplicate.metadata","version":{"major":1,"minor":0},"operations":[]}]}
            """));
        ConnectorProviderManifest second = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"duplicate.metadata","version":{"major":2,"minor":0},"operations":[]}]}
            """));
        IllegalArgumentException duplicate = assertThrows(
            IllegalArgumentException.class, () -> new ConnectorProviderManifestCatalog(List.of(first, second)));
        assertEquals("duplicate connector provider ID in static metadata: duplicate.metadata", duplicate.getMessage());
    }

    @Test
    void rejectsDuplicateSchemaVersionAndOperationIdentity() {
        IllegalArgumentException duplicateField = assertThrows(IllegalArgumentException.class, () -> ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"schemaVersion":1,"providers":[]}
            """)));
        assertTrue(duplicateField.getMessage().contains("duplicate field 'schemaVersion'"));

        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"duplicate.operation","version":{"major":1,"minor":0},"operations":[
            {"id":"find","kind":"tpf:query","majorVersion":1},{"id":"find","kind":"tpf:query","majorVersion":1}]}]}
            """));
        IllegalArgumentException duplicateOperation = assertThrows(
            IllegalArgumentException.class, () -> new ConnectorProviderManifestCatalog(List.of(manifest)));
        assertTrue(duplicateOperation.getMessage().contains("duplicate connector operation identity"));
    }

    @Test
    void supportsUnicodeEscapesAndRejectsNullValues() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"unicode\\u002eprovider","version":{"major":1,"minor":0},"operations":[]}]}
            """));
        assertEquals("unicode.provider", manifest.providers().getFirst().provider().id().value());

        IllegalArgumentException nullValue = assertThrows(IllegalArgumentException.class, () -> ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":null,"providers":[]}
            """)));
        assertTrue(nullValue.getMessage().contains("null values are not supported"));
    }

    @Test
    void rejectsFrameworkReservedProviderIDsInExternalStaticMetadata() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class, () -> ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"tpf.external","version":{"major":1,"minor":0},"operations":[]}]}
            """)));

        assertEquals("connector provider ID is reserved for framework use: tpf.external", rejected.getMessage());
    }

    @Test
    void rejectsDeletedExecutionCapabilitiesField() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":3,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "executionCapabilities":{"executionStyle":"NON_BLOCKING","concurrencyScope":"PROVIDER_MANAGED"},
                "operations":[]}]}
                """)));
        assertEquals("malformed connector provider manifest: unsupported field 'executionCapabilities'", rejected.getMessage());
    }

    @Test
    void readsCommandExecutionPostureAndDefaultsUndeclaredPostureConservatively() {
        ConnectorProviderManifest declared = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"write","kind":"tpf:command","majorVersion":1,
            "commandCapabilities":{"retryRedriveSupported":false,"providerIdempotencySupported":false,
            "reconciliationSupported":false,"executionPosture":"AUTOMATED","maximumMachineConfirmation":"NONE",
            "userConfirmationSupported":false,"durableReferenceKinds":[]}}]}]}
            """));
        assertEquals(CommandExecutionPosture.AUTOMATED, declared.providers().getFirst().operations().getFirst()
            .commandCapabilities().orElseThrow().executionPosture());

        ConnectorProviderManifest undeclared = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"write","kind":"tpf:command","majorVersion":1,
            "commandCapabilities":{"retryRedriveSupported":false,"providerIdempotencySupported":false,
            "reconciliationSupported":false,"maximumMachineConfirmation":"NONE",
            "userConfirmationSupported":false,"durableReferenceKinds":[]}}]}]}
            """));
        assertEquals(CommandExecutionPosture.UNSPECIFIED, undeclared.providers().getFirst().operations().getFirst()
            .commandCapabilities().orElseThrow().executionPosture());

        IllegalArgumentException invalid = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[{"id":"write","kind":"tpf:command","majorVersion":1,
                "commandCapabilities":{"retryRedriveSupported":false,"providerIdempotencySupported":false,
                "reconciliationSupported":false,"executionPosture":"ROBOT","maximumMachineConfirmation":"NONE",
                "userConfirmationSupported":false,"durableReferenceKinds":[]}}]}]}
                """)));
        assertEquals("malformed connector provider manifest: field 'executionPosture' must be a CommandExecutionPosture",
            invalid.getMessage());
    }

    @Test
    void readsMinimalQueryCapabilitiesAndDefaultsUndeclaredValuesConservatively() {
        ConnectorProviderManifest declared = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1,
            "queryCapabilities":{"cacheability":"CACHEABLE","maximumCacheAge":"PT10M",
            "maximumNegativeCacheTtl":"PT30S"}}]}]}
            """));
        QueryCapabilities capabilities = declared.providers().getFirst().operations().getFirst()
            .queryCapabilities().orElseThrow();
        assertEquals(QueryCacheability.CACHEABLE, capabilities.cacheability());
        assertEquals(Duration.ofMinutes(10), capabilities.maximumCacheAge().orElseThrow());
        assertEquals(Duration.ofSeconds(30), capabilities.maximumNegativeCacheTtl().orElseThrow());

        ConnectorProviderManifest undeclared = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1}]}]}
            """));
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("metadata.provider"), "find", ConnectorOperationKind.QUERY, 1);
        assertEquals(
            QueryCapabilities.conservative(),
            new ConnectorProviderManifestCatalog(List.of(undeclared)).requireQueryCapabilities(identity, 1));
    }

    @Test
    void schemaFourRequiresAndReadsStructuralQueryCardinality() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":4,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"find.many","kind":"tpf:query","majorVersion":1,
            "queryCardinality":"ONE_TO_MANY"}]}]}
            """));

        assertEquals(QueryOperationCardinality.ONE_TO_MANY,
            manifest.providers().getFirst().operations().getFirst().queryCardinality().orElseThrow());

        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":4,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[{"id":"find.one","kind":"tpf:query","majorVersion":1}]}]}
                """)));
        assertTrue(missing.getMessage().contains("queryCardinality"));

        IllegalArgumentException nonQuery = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":4,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[{"id":"send","kind":"tpf:command","majorVersion":1,
                "queryCardinality":"ONE_TO_MANY"}]}]}
                """)));
        assertTrue(nonQuery.getMessage().contains("valid only for Query operations"));

        IllegalArgumentException unaryCache = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":4,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "operations":[{"id":"find.many","kind":"tpf:query","majorVersion":1,
                "queryCardinality":"ONE_TO_MANY","queryCapabilities":{"cacheability":"LIVE_ONLY"}}]}]}
                """)));
        assertTrue(unaryCache.getMessage().contains("must not declare unary Query cache capabilities"));
    }

    @Test
    void rejectsCommandIdentityWhenQueryCapabilitiesAreRequested() {
        ConnectorProviderManifest manifest = ConnectorProviderManifestReader.read(input("""
            {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
            "operations":[{"id":"find","kind":"tpf:query","majorVersion":1}]}]}
            """));
        ConnectorOperationIdentity commandIdentity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("metadata.provider"), "find", ConnectorOperationKind.COMMAND, 1);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new ConnectorProviderManifestCatalog(List.of(manifest))
                .requireQueryCapabilities(commandIdentity, 1));

        assertTrue(failure.getMessage().contains("require a Query operation identity"));
    }

    private static ByteArrayInputStream input(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }
}
