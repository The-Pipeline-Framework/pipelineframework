package org.pipelineframework.connector.openapi.maven;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.http.HttpOperationCatalog;
import static org.junit.jupiter.api.Assertions.*;

class OpenApiCallbackImportTest {
    @TempDir Path directory;

    @Test
    void importsBodyQueryHeaderAndEscapedObjectFieldsWithProviderAgreement() throws Exception {
        Map<String, String> targets = Map.of(
            "{$request.body#/callbackUrl}", "/body/callbackUrl",
            "{$request.body#/call~1back~0url}", "/body/call~1back~0url",
            "{$request.query.callbackQuery}", "/callbackQuery",
            "{$request.header.X-Callback}", "/callbackHeader");
        for (var target : targets.entrySet()) {
            var imported = imported(target.getKey(), ignored -> { });
            var pins = HttpOperationCatalog.read(resource(imported, "http-operations.json"));
            var callback = pins.operations().getFirst().callbacks().getFirst();
            assertEquals(target.getValue(), callback.target().pointer());
            assertEquals("JobCallback", callback.inputType());
            assertEquals(202, callback.acknowledgementStatus());
            assertEquals("callbackSignature", callback.security().requirements().getFirst().scheme());
            var manifest = new org.pipelineframework.connector.ConnectorProviderManifest(
                org.pipelineframework.connector.ConnectorProviderManifest.CURRENT_SCHEMA_VERSION,
                List.of(imported.provider()));
            pins.validateCallbacks(new ConnectorProviderManifestCatalog(List.of(manifest)));
            assertEquals(callback.descriptor(), manifest.providers().getFirst().operations().getFirst().callbacks().getFirst());
            String provenance = resource(imported, "connector-operation-provenance.json");
            assertTrue(provenance.contains("sourceExpression"));
            assertTrue(provenance.contains("injectionTargetFingerprint"));
            assertTrue(provenance.contains("jobCompleted"));
            assertTrue(imported.discoveryReport().contains("independentEvent"));
            assertTrue(imported.resources().stream().noneMatch(value -> value.content().contains("https://vendor.invalid")));
        }
    }

    @Test
    void rejectsExpressionsThatCannotBeInjectedBeforeDispatch() {
        for (String expression : List.of("{$response.body#/url}", "{$url}", "https://provider.invalid/callback",
            "{$request.body#/items/0}", "{$request.body#/items/*}", "{$request.body#/callback~2Url}",
            "{$request.query.absent}", "{$request.header.Authorization}")) {
            assertThrows(IllegalArgumentException.class, () -> imported(expression, ignored -> { }), expression);
        }
    }

    @Test
    void rejectsDriftAndUnselectedSecurityMediaAndAcknowledgement() {
        List<Consumer<OpenApiImportConfiguration.CallbackSelection>> changes = List.of(
            callback -> callback.source.name = "missing",
            callback -> callback.source.operationId = "different",
            callback -> callback.source.method = "GET",
            callback -> callback.request.mediaType = "text/plain",
            callback -> callback.acknowledgement.status = "200",
            callback -> callback.acknowledgement.status = "400",
            callback -> callback.security.require = Map.of("other", List.of()));
        for (var change : changes) {
            assertThrows(IllegalArgumentException.class, () -> imported("{$request.body#/callbackUrl}", change));
        }
    }

    private String resource(OpenApiImportEngine.ImportedArtifacts artifacts, String name) {
        return artifacts.resources().stream().filter(value -> value.path().endsWith("/" + name))
            .findFirst().orElseThrow().content();
    }

    private OpenApiImportEngine.ImportedArtifacts imported(String expression,
        Consumer<OpenApiImportConfiguration.CallbackSelection> change) throws Exception {
        Files.writeString(directory.resolve("callback.yaml"), """
            Callback:
              type: object
              properties:
                status: {type: string}
              required: [status]
              additionalProperties: false
            """);
        Path source = directory.resolve("openapi.yaml");
        Files.writeString(source, CONTRACT.formatted(expression));
        var closure = OpenApiContractClosure.load(source);
        var config = new OpenApiImportConfiguration();
        config.schemaVersion = 1;
        config.importId = "jobs";
        config.source = new OpenApiImportConfiguration.Source();
        config.source.snapshot = "openapi.yaml";
        config.source.closureSha256 = closure.digest();
        var operation = new OpenApiImportConfiguration.OperationSelection();
        operation.source = new OpenApiImportConfiguration.SourceOperation();
        operation.source.path = "/jobs";
        operation.source.method = "POST";
        operation.source.operationId = "startJob";
        operation.operation = "job.start";
        operation.version = 1;
        operation.kind = "COMMAND";
        operation.input = "StartJobRequest";
        operation.output = "JobAccepted";
        operation.server = "APPLICATION_BOUND";
        operation.security = new OpenApiImportConfiguration.SecuritySelection();
        operation.security.none = true;
        operation.request = new OpenApiImportConfiguration.RequestSelection();
        operation.request.mediaType = "application/json";
        operation.request.representation = "http.job.start.request";
        operation.request.parameterSources = Map.of("X-Callback", "callbackHeader");
        var response = new OpenApiImportConfiguration.ResponseSelection();
        response.status = "202";
        response.mediaType = "application/json";
        response.outcome = "SUCCEEDED";
        response.representation = "http.job.accepted";
        response.confirmation = "PROVIDER_ACKNOWLEDGED";
        operation.responses = List.of(response);
        var callback = new OpenApiImportConfiguration.CallbackSelection();
        callback.source = new OpenApiImportConfiguration.SourceCallback();
        callback.source.name = "jobStatus";
        callback.source.expression = expression;
        callback.source.method = "POST";
        callback.source.operationId = "jobCompleted";
        callback.callback = "job.completed";
        callback.input = "JobCallback";
        callback.request = new OpenApiImportConfiguration.RequestSelection();
        callback.request.mediaType = "application/json";
        callback.request.representation = "http.job.completed.request";
        callback.acknowledgement = new OpenApiImportConfiguration.AcknowledgementSelection();
        callback.acknowledgement.status = "202";
        callback.security = new OpenApiImportConfiguration.SecuritySelection();
        callback.security.require = Map.of("callbackSignature", List.of());
        operation.callbacks = List.of(callback);
        config.operations = List.of(operation);
        change.accept(callback);
        return new OpenApiImportEngine().importContract(new AbstractOpenApiImportMojo.LoadedImport(
            directory.resolve("import.yaml"), config, closure));
    }

    private static final String CONTRACT = """
        openapi: 3.1.2
        info: {title: Jobs, version: '1'}
        servers: [{url: 'https://vendor.invalid'}]
        webhooks:
          independentEvent:
            post:
              operationId: independentNotification
              responses: {'202': {description: accepted}}
        paths:
          /jobs:
            post:
              operationId: startJob
              security: []
              parameters:
                - {name: callbackQuery, in: query, schema: {type: string, format: uri}}
                - {name: X-Callback, in: header, schema: {type: string, format: uri}}
              requestBody:
                required: true
                content:
                  application/json:
                    schema:
                      type: object
                      properties:
                        value: {type: string}
                        callbackUrl: {type: string, format: uri}
                        call/back~url: {type: string, format: uri}
                      required: [value, callbackUrl]
                      additionalProperties: false
              responses:
                '202':
                  description: accepted
                  content:
                    application/json:
                      schema: {type: object, properties: {accepted: {type: boolean}}}
              callbacks:
                jobStatus:
                  '%s':
                    post:
                      operationId: jobCompleted
                      security: [{callbackSignature: []}]
                      requestBody:
                        required: true
                        content:
                          application/json:
                            schema: {$ref: './callback.yaml#/Callback'}
                      responses:
                        '202': {description: recorded}
        components:
          securitySchemes:
            callbackSignature: {type: apiKey, in: header, name: X-Callback-Signature}
        """;
}
