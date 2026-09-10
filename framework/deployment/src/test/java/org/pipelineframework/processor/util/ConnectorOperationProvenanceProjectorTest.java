package org.pipelineframework.processor.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;

class ConnectorOperationProvenanceProjectorTest {
    @TempDir Path temporary;

    @Test
    void projectsOnlyReleaseTimeImportsReferencedByOrdinaryAndCallableOperations() throws Exception {
        Path pipeline = temporary.resolve("pipeline.yaml");
        Files.writeString(pipeline, """
            version: 3
            appName: provenance-proof
            basePackage: example
            connectors:
              vendor-http: { provider: http.client, version: 1, config: { connection: vendor } }
            types:
              Input: { fields: [[subject, string]] }
              Output: { fields: [[value, string]] }
              Decision: { fields: [[value, string]] }
            steps:
              - name: Lookup
                kind: query
                using: vendor-http
                operation: evidence.lookup
                operationVersion: 1
                input: Input
                output: Output
              - name: Decide
                kind: query
                using: vendor-http
                operation: evidence.lookup
                operationVersion: 1
                input: Input
                output: Decision
                callables:
                  record:
                    using: vendor-http
                    operation: evidence.record
                    operationVersion: 1
                    kind: command
                    input: Input
            """);
        Path resource = temporary.resolve("src/main/resources")
            .resolve(ConnectorOperationProvenanceProjector.RESOURCE_PATH);
        Files.createDirectories(resource.getParent());
        Files.writeString(resource, provenance("evidence.lookup", "evidence.record", "evidence.unused"));
        var config = new PipelineYamlConfigLoader().load(pipeline);

        List<Map<String, Object>> projected = new ConnectorOperationProvenanceProjector()
            .project(config, temporary, getClass().getClassLoader(), List.of());

        assertEquals(1, projected.size());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> operations = (List<Map<String, Object>>) projected.getFirst().get("operations");
        assertEquals(List.of("evidence.record", "evidence.lookup"), operations.stream()
            .map(value -> value.get("operation").toString()).toList());
        assertEquals(List.of(1, 1), operations.stream().map(value -> value.get("majorVersion")).toList());
    }

    @Test
    void rejectsConflictingProvenanceForTheSameImport() throws Exception {
        Path pipeline = temporary.resolve("pipeline.yaml");
        Files.writeString(pipeline, """
            version: 3
            appName: provenance-proof
            basePackage: example
            connectors:
              vendor-http: { provider: http.client, version: 1, config: { connection: vendor } }
            types: { Input: { fields: [[subject, string]] }, Output: { fields: [[value, string]] } }
            steps:
              - name: Lookup
                kind: query
                using: vendor-http
                operation: evidence.lookup
                operationVersion: 1
                input: Input
                output: Output
            """);
        Path resource = temporary.resolve("src/main/resources")
            .resolve(ConnectorOperationProvenanceProjector.RESOURCE_PATH);
        Files.createDirectories(resource.getParent());
        Files.writeString(resource, "{\"schemaVersion\":2,\"imports\":[]}");
        var config = new PipelineYamlConfigLoader().load(pipeline);

        assertThrows(IllegalStateException.class, () -> new ConnectorOperationProvenanceProjector()
            .project(config, temporary, getClass().getClassLoader(), List.of()));
    }

    @Test
    void rejectsFractionalAndOutOfRangeVersionNumbers() throws Exception {
        var config = singleLookupConfig();
        Path resource = temporary.resolve("src/main/resources")
            .resolve(ConnectorOperationProvenanceProjector.RESOURCE_PATH);
        Files.createDirectories(resource.getParent());

        Files.writeString(resource, "{\"schemaVersion\":1.5,\"imports\":[]}");
        assertThrows(IllegalStateException.class, () -> new ConnectorOperationProvenanceProjector()
            .project(config, temporary, getClass().getClassLoader(), List.of()));

        Files.writeString(resource, """
            {"schemaVersion":1,"imports":[{"provider":"http.client","importId":"evidence-api",
              "operations":[{"kind":"tpf:query","majorVersion":2147483648,"operation":"evidence.lookup"}]}]}
            """);
        assertThrows(IllegalStateException.class, () -> new ConnectorOperationProvenanceProjector()
            .project(config, temporary, getClass().getClassLoader(), List.of()));
    }

    private org.pipelineframework.config.pipeline.PipelineYamlConfig singleLookupConfig() throws Exception {
        Path pipeline = temporary.resolve("single-lookup-pipeline.yaml");
        Files.writeString(pipeline, """
            version: 3
            appName: provenance-proof
            basePackage: example
            connectors:
              vendor-http: { provider: http.client, version: 1, config: { connection: vendor } }
            types: { Input: { fields: [[subject, string]] }, Output: { fields: [[value, string]] } }
            steps:
              - name: Lookup
                kind: query
                using: vendor-http
                operation: evidence.lookup
                operationVersion: 1
                input: Input
                output: Output
            """);
        return new PipelineYamlConfigLoader().load(pipeline);
    }

    private static String provenance(String... operations) {
        String joined = java.util.Arrays.stream(operations).map(operation -> """
            {"kind":"%s","majorVersion":1,"operation":"%s"}
            """.formatted(operation.endsWith("record") ? "tpf:command" : "tpf:query", operation).trim())
            .collect(java.util.stream.Collectors.joining(","));
        return """
            {"schemaVersion":1,"imports":[{"provider":"http.client","importId":"evidence-api",
              "sourceKind":"OPENAPI","operations":[%s]}]}
            """.formatted(joined);
    }
}
