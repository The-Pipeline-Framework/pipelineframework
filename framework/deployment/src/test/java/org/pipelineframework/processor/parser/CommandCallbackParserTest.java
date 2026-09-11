package org.pipelineframework.processor.parser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.StepDefinition;
import static org.junit.jupiter.api.Assertions.*;

class CommandCallbackParserTest {
    @TempDir Path directory;

    private static final String YAML = """
        version: 3
        basePackage: com.example
        connectors:
          jobs:
            provider: test.jobs
            version: 1
        steps:
          - name: Start job
            kind: command
            operation: start
            using: jobs
            cardinality: ONE_TO_ONE
            input: Input
            output: Final
            java:
              input: com.example.Input
              output: com.example.Final
            commandIdGenerator: com.example.IdGenerator
            duplicatePolicy: RETURN_RECORDED
            await:
              operationOutput:
                type: Accepted
                java: com.example.Accepted
              timeout: PT1M
              correlation:
                strategy: signedResumeToken
              callback:
                name: completed
                endpointResolver: com.example.EndpointResolver
                authenticator: com.example.Authenticator
              completion:
                type: Completed
                projector: com.example.Projector
        """;

    @Test
    void preservesSeparateImmediateAndFinalContractsWithNativeCommandAuthority() throws Exception {
        var result = parse(YAML);
        assertEquals(1, result.steps().size(), result.errors().toString());
        var step = result.steps().getFirst();
        assertEquals("com.example.Final", step.outputType().canonicalName());
        var completion = step.deferredCompletion().orElseThrow();
        assertEquals("Accepted", completion.operationOutputType());
        assertEquals("completed", completion.callback().orElseThrow().name());
        assertEquals("jobs", step.connectorOperationSelection().orElseThrow().binding().value());
        assertTrue(result.errors().isEmpty(), result.errors().toString());
    }

    @Test
    void resolvesCanonicalAcknowledgementWithoutAnExplicitJavaOverride() throws Exception {
        String yaml = YAML.replace("connectors:", """
            appName: Callback
            types:
              Input: { fields: [[id, string]] }
              Accepted: { fields: [[id, string]] }
              Completed: { fields: [[id, string]] }
              Final: { fields: [[id, string]] }
            connectors:""")
            .replace("        java: com.example.Accepted\n", "");
        var result = parse(yaml);
        assertEquals(1, result.steps().size(), result.errors().toString());
        assertEquals("Accepted", result.steps().getFirst().deferredCompletion().orElseThrow().operationOutputType());
    }

    @Test
    void rejectsUnsupportedCallbackShapesAndContractMismatches() throws Exception {
        for (String invalid : List.of(
            YAML.replace("type: Accepted", "type: Wrong").replace("com.example.Accepted", "com.example.Wrong"),
            YAML.replace("type: Completed", "type: Wrong"),
            YAML.replace("name: completed", "name: absent"),
            YAML.replace("name: completed", "name: ''"),
            YAML.replace("name: completed", "name: 42"),
            YAML.replace("signedResumeToken", "interactionId"),
            YAML.replace("cardinality: ONE_TO_ONE", "cardinality: ONE_TO_MANY"),
            YAML.replace("kind: command", "kind: query"),
            YAML.replace("      authenticator: com.example.Authenticator\n", ""),
            YAML.replace("      callback:", "      transport: {type: interaction-api}\n      callback:"),
            YAML.replace("      callback:", "      idempotency: {fields: [id]}\n      callback:"),
            YAML.replace("    operation: start\n    using: jobs", "    command: legacy.command")
        )) {
            var result = parse(invalid);
            assertTrue(result.steps().isEmpty(), invalid);
            assertFalse(result.errors().isEmpty(), invalid);
        }
    }

    @Test
    void callbackCapableCommandCannotSilentlyRunSynchronously() throws Exception {
        var result = parse(YAML.substring(0, YAML.indexOf("    await:"))
            .replace("output: Final", "output: Accepted")
            .replace("output: com.example.Final", "output: com.example.Accepted"));
        assertTrue(result.steps().isEmpty());
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("requires await.callback")), result.errors().toString());
    }

    private Parsed parse(String yaml) throws Exception {
        Path manifest = directory.resolve("META-INF/pipeline/connector-providers.json");
        Files.createDirectories(manifest.getParent());
        Files.writeString(manifest, """
            {"schemaVersion":7,"providers":[{"id":"test.jobs","version":{"major":1,"minor":0},"operations":[
              {"id":"start","kind":"tpf:command","majorVersion":1,
               "typeContract":{"input":"Input","output":"Accepted"},
               "callbacks":[{"id":"completed","typeContract":{"input":"Completed"},"required":true}]}]}]}
            """);
        Path config = directory.resolve("pipeline.yaml");
        Files.writeString(config, yaml);
        List<String> errors = new ArrayList<>();
        try (var loader = new URLClassLoader(new URL[] {directory.toUri().toURL()}, ClassLoader.getPlatformClassLoader())) {
            var steps = new StepDefinitionParser((kind, message) -> {
                if (kind == javax.tools.Diagnostic.Kind.ERROR) { errors.add(message); }
            }, StepDefinitionParser.DEFAULT_LEGACY_INTERNAL_PACKAGE_SUFFIX, loader).parseStepDefinitions(config);
            return new Parsed(steps, errors);
        }
    }

    private record Parsed(List<StepDefinition> steps, List<String> errors) { }
}
