package org.pipelineframework.connector.openapi.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.http.HttpOperationCatalog;

class OpenApiImportEngineTest {
    @TempDir
    Path temporary;
    private Path snapshot;
    private OpenApiContractClosure.Resolved closure;

    @BeforeEach
    void contract() throws Exception {
        Files.writeString(temporary.resolve("schemas.yaml"), SCHEMAS);
        snapshot = temporary.resolve("openapi.yaml");
        Files.writeString(snapshot, CONTRACT);
        closure = OpenApiContractClosure.load(snapshot);
    }

    @Test
    void importsExplicitPostQueryAndCommandIntoOrdinaryHttpPins() {
        OpenApiImportConfiguration configuration = configuration();
        var imported = new OpenApiImportEngine().importContract(new AbstractOpenApiImportMojo.LoadedImport(
            temporary.resolve("openapi-import.yaml"), configuration, closure));

        HttpOperationCatalog pins = HttpOperationCatalog.read(imported.resources().stream()
            .filter(value -> value.path().equals(OpenApiImportEngine.PIN_PATH)).findFirst().orElseThrow().content());
        assertEquals(Set.of(ConnectorOperationKind.QUERY, ConnectorOperationKind.COMMAND), pins.operations().stream()
            .map(value -> value.kind()).collect(Collectors.toSet()));
        var query = pins.operations().stream()
            .filter(value -> value.kind().equals(ConnectorOperationKind.QUERY))
            .findFirst().orElseThrow();
        assertEquals(List.of("subject", "verbose"), query.parameters().stream()
            .map(value -> value.name()).toList());
        assertEquals("body", query.requestBody().orElseThrow().sourcePath().orElseThrow());
        assertTrue(imported.discoveryReport().contains("lookupEvidence"));
        assertTrue(imported.resources().stream().filter(value -> value.path().equals(OpenApiImportEngine.PROVENANCE_PATH))
            .findFirst().orElseThrow().content().contains(closure.digest()));
    }

    @Test
    void neverInfersAuthorityFromHttpMethod() {
        OpenApiImportConfiguration configuration = configuration();
        configuration.operations.getFirst().kind = null;

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new OpenApiImportEngine().importContract(new AbstractOpenApiImportMojo.LoadedImport(
                temporary.resolve("openapi-import.yaml"), configuration, closure)));

        assertTrue(failure.getMessage().contains("kind"));
    }

    @Test
    void pinnedPetstoreLogoutProvesThatEffectfulGetDoesNotGrantQueryAuthority() throws Exception {
        Path petstore = Path.of(getClass().getResource("/petstore3-openapi.yaml").toURI());
        OpenApiContractClosure.Resolved petstoreClosure = OpenApiContractClosure.load(petstore);
        assertEquals("3.0.4", petstoreClosure.version());
        assertEquals("b223c466466f8a63c4b2cb115731cbeea5891476d62c07117d155acff0476d01",
            petstoreClosure.acquiredClosureDigest());
        assertTrue(new OpenApiImportEngine().discover(petstoreClosure).contains("logoutUser"));

        OpenApiImportConfiguration configuration = new OpenApiImportConfiguration();
        configuration.schemaVersion = 1;
        configuration.importId = "petstore-conformance";
        configuration.source = new OpenApiImportConfiguration.Source();
        configuration.source.snapshot = "petstore3-openapi.yaml";
        var logout = new OpenApiImportConfiguration.OperationSelection();
        logout.source = new OpenApiImportConfiguration.SourceOperation();
        logout.source.operationId = "logoutUser";
        logout.source.method = "GET";
        logout.source.path = "/user/logout";
        logout.operation = "petstore.logout";
        logout.version = 1;
        logout.kind = null;
        logout.input = "LogoutRequest";
        logout.output = "LogoutResult";
        logout.server = "APPLICATION_BOUND";
        logout.request = new OpenApiImportConfiguration.RequestSelection();
        logout.request.representation = "http.petstore.logout.request";
        logout.security = new OpenApiImportConfiguration.SecuritySelection();
        logout.security.none = true;
        logout.responses = List.of(response("200", null, "EMPTY"));
        configuration.operations = List.of(logout);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> new OpenApiImportEngine().importContract(new AbstractOpenApiImportMojo.LoadedImport(
                temporary.resolve("petstore-import.yaml"), configuration, petstoreClosure)));
        assertTrue(failure.getMessage().contains("kind"));
    }

    @Test
    void enforcesVersionDigestAndOfflineReferenceClosure() throws Exception {
        OpenApiImportConfiguration configuration = configuration();
        configuration.source.closureSha256 = "0".repeat(64);
        assertThrows(IllegalArgumentException.class, () -> new OpenApiImportEngine().importContract(
            new AbstractOpenApiImportMojo.LoadedImport(temporary.resolve("import.yaml"), configuration, closure)));
        Files.writeString(snapshot, CONTRACT.replace("3.1.2", "3.0.4"));
        assertEquals("3.0.4", OpenApiContractClosure.load(snapshot).version());
        Files.writeString(snapshot, CONTRACT.replace("3.1.2", "3.2.0"));
        assertThrows(IllegalArgumentException.class, () -> OpenApiContractClosure.load(snapshot));
        Files.writeString(snapshot, CONTRACT.replace("schemas.yaml", "https://vendor.invalid/schemas.yaml"));
        assertThrows(IllegalArgumentException.class, () -> OpenApiContractClosure.load(snapshot));
    }

    @Test
    void rejectsCyclicExternalReferenceClosures() throws Exception {
        Files.writeString(temporary.resolve("a.yaml"), "$ref: ./b.yaml#/B\n");
        Files.writeString(temporary.resolve("b.yaml"), "$ref: ./a.yaml#/A\n");
        Files.writeString(snapshot, """
            openapi: 3.1.2
            info: { title: Cycle, version: '1' }
            paths: {}
            components: { schemas: { A: { $ref: './a.yaml#/A' } } }
            """);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> OpenApiContractClosure.load(snapshot));
        assertTrue(failure.getMessage().contains("cycle"));
    }

    private OpenApiImportConfiguration configuration() {
        var configuration = new OpenApiImportConfiguration();
        configuration.schemaVersion = 1;
        configuration.importId = "evidence-api";
        configuration.source = new OpenApiImportConfiguration.Source();
        configuration.source.snapshot = "openapi.yaml";
        configuration.source.closureSha256 = closure.digest();
        var query = selection("lookupEvidence", "/evidence/{subject}/lookup", "evidence.lookup", "QUERY",
            "LookupArguments", "LookupResult", "200", "RESULT");
        query.request.parameterSources = Map.of("subject", "subject", "verbose", "verbose");
        query.security.require = Map.of("oauth2", List.of("evidence.read"));
        var empty = response("404", null, "EMPTY");
        empty.code = "not-found";
        query.responses = List.of(query.responses.getFirst(), empty);
        var command = selection("recordEvidence", "/evidence", "evidence.record", "COMMAND",
            "RecordArguments", "RecordResult", "202", "SUCCEEDED");
        command.security.require = Map.of("apiKey", List.of());
        command.providerIdempotencyKey = new OpenApiImportConfiguration.ProviderIdempotencyKey();
        command.providerIdempotencyKey.location = "HEADER";
        command.providerIdempotencyKey.name = "Idempotency-Key";
        configuration.operations = List.of(query, command);
        return configuration;
    }

    private static OpenApiImportConfiguration.OperationSelection selection(
        String sourceId, String path, String operation, String kind, String input, String output,
        String status, String outcome
    ) {
        var selection = new OpenApiImportConfiguration.OperationSelection();
        selection.source = new OpenApiImportConfiguration.SourceOperation();
        selection.source.operationId = sourceId;
        selection.source.method = "POST";
        selection.source.path = path;
        selection.operation = operation;
        selection.version = 1;
        selection.kind = kind;
        selection.input = input;
        selection.output = output;
        selection.server = "APPLICATION_BOUND";
        selection.request = new OpenApiImportConfiguration.RequestSelection();
        selection.request.mediaType = "application/json";
        selection.request.representation = "http." + operation + ".request";
        selection.security = new OpenApiImportConfiguration.SecuritySelection();
        var response = response(status, "application/json", outcome);
        response.representation = "http." + operation + ".response";
        response.confirmation = "PROVIDER_ACKNOWLEDGED";
        selection.responses = List.of(response);
        return selection;
    }

    private static OpenApiImportConfiguration.ResponseSelection response(String status, String media, String outcome) {
        var response = new OpenApiImportConfiguration.ResponseSelection();
        response.status = status;
        response.mediaType = media;
        response.outcome = outcome;
        return response;
    }

    private static final String CONTRACT = """
        openapi: 3.1.2
        info: { title: Evidence API, version: 1.0.0 }
        servers:
          - url: https://{tenant}.vendor.invalid/v1
            variables: { tenant: { default: never-trusted } }
        components:
          securitySchemes:
            oauth2:
              type: oauth2
              flows: { clientCredentials: { tokenUrl: https://auth.vendor.invalid/token, scopes: { evidence.read: Read } } }
            apiKey: { type: apiKey, in: header, name: X-Api-Key }
        paths:
          /evidence/{subject}/lookup:
            post:
              operationId: lookupEvidence
              security: [ { oauth2: [ evidence.read ] } ]
              parameters:
                - { name: subject, in: path, required: true, schema: { type: string } }
                - { name: verbose, in: query, schema: { type: boolean } }
              requestBody:
                required: true
                content: { application/json: { schema: { $ref: './schemas.yaml#/components/schemas/LookupBody' } } }
              responses:
                '200': { description: Found, content: { application/json: { schema: { $ref: './schemas.yaml#/components/schemas/LookupResult' } } } }
                '404': { description: Missing }
          /evidence:
            post:
              operationId: recordEvidence
              security: [ { apiKey: [] } ]
              requestBody:
                required: true
                content: { application/json: { schema: { $ref: './schemas.yaml#/components/schemas/RecordBody' } } }
              responses:
                '202': { description: Recorded, content: { application/json: { schema: { $ref: './schemas.yaml#/components/schemas/RecordResult' } } } }
        """;

    private static final String SCHEMAS = """
        components:
          schemas:
            LookupBody: { type: object, additionalProperties: false, properties: { key: { type: string } }, required: [ key ] }
            LookupResult: { type: object, additionalProperties: false, properties: { value: { type: string } }, required: [ value ] }
            RecordBody: { type: object, additionalProperties: false, properties: { effectKey: { type: string }, value: { type: string } }, required: [ effectKey, value ] }
            RecordResult: { type: object, additionalProperties: false, properties: { receipt: { type: string } }, required: [ receipt ] }
        """;
}
