package org.pipelineframework.connector;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void reportsMalformedExecutionCapabilitiesWithManifestDiagnostics() {
        IllegalArgumentException executionStyle = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "executionCapabilities":{"executionStyle":"ASYNC","concurrencyScope":"PROVIDER_MANAGED"},"operations":[]}]}
                """)));
        assertEquals("malformed connector provider manifest: field 'executionStyle' must be a ConnectorExecutionStyle",
            executionStyle.getMessage());

        IllegalArgumentException concurrencyScope = assertThrows(IllegalArgumentException.class,
            () -> ConnectorProviderManifestReader.read(input("""
                {"schemaVersion":1,"providers":[{"id":"metadata.provider","version":{"major":1,"minor":0},
                "executionCapabilities":{"executionStyle":"NON_BLOCKING","concurrencyScope":"BOUNDED"},"operations":[]}]}
                """)));
        assertEquals("malformed connector provider manifest: field 'concurrencyScope' must be a ConnectorConcurrencyScope",
            concurrencyScope.getMessage());
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
