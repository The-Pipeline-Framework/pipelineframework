package org.pipelineframework.processor.representation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;

class CanonicalSchemaJsonTest {
    @TempDir Path tempDir;

    @Test
    void addsNullToNullableUnionFields() throws Exception {
        Path yaml = tempDir.resolve("pipeline.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: nullable-union-schema
            basePackage: example
            types:
              Result: { fields: [[value, string]] }
              Decision:
                variants:
                  result: Result
              Envelope:
                fields:
                  - name: decision
                    type: Decision
                    presence: required
                    nullability: nullable
            steps: []
            """);
        var model = new PipelineTemplateConfigLoader().load(yaml).typeModel();

        var schema = PipelineJson.mapper().readTree(CanonicalSchemaJson.render(model, "Envelope"));
        var alternatives = schema.path("properties").path("decision").path("oneOf");

        assertTrue(alternatives.isArray());
        assertTrue(java.util.stream.StreamSupport.stream(alternatives.spliterator(), false)
            .anyMatch(candidate -> "null".equals(candidate.path("type").asText())));
    }
}
