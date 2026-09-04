package org.pipelineframework.processor.block;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;

class BlockDefinitionImporterTest {
    @TempDir Path directory;

    @Test void normalizesAndValidatesManifestRequirementNamesBeforeCopying() {
        var requirements = new LinkedHashMap<String, BlockPackageManifest.Requirement>();
        requirements.put(" graphql.read ", new BlockPackageManifest.Requirement("query"));
        var definition = new BlockPackageManifest.Definition("lookup", "definition.yaml", requirements);
        assertTrue(definition.requires().containsKey("graphql.read"));

        requirements.put("invalid", null);
        var invalid = assertThrows(IllegalArgumentException.class,
            () -> new BlockPackageManifest.Definition("lookup", "definition.yaml", requirements));
        assertTrue(invalid.getMessage().contains("entry must not be null"));
    }

    @Test void discoversDependencyMetadataAndLinksOrdinaryV3Definitions() throws Exception {
        Path dependency = dependency("first", "org.example.documents", "documents", "1.2.3",
            "document-text-extraction", """
                version: 3
                types:
                  DocumentFile:
                    java: org.example.DocumentFile
                    fields: [[content, payload_ref]]
                  ExtractedDocument:
                    java: org.example.ExtractedDocument
                    fields: [[text, string]]
                pipelines:
                  document-text-extraction:
                    input: DocumentFile
                    output: ExtractedDocument
                    steps:
                      - { name: Extract, service: org.example.ExtractService, cardinality: ONE_TO_ONE, input: DocumentFile, output: ExtractedDocument, java: { input: org.example.DocumentFile, output: org.example.ExtractedDocument } }
                """);
        Path application = application("""
            version: 3
            appName: Consumer
            basePackage: org.example.consumer
            transport: LOCAL
            platform: COMPUTE
            contract: { input: DocumentFile, output: ExtractedDocument }
            types:
              LocalReceipt: { fields: [[value, string]] }
            steps:
              - { name: Extract, pipeline: document-text-extraction, cardinality: ONE_TO_ONE, input: DocumentFile, output: ExtractedDocument, java: { input: org.example.DocumentFile, output: org.example.ExtractedDocument } }
            """);

        try (URLClassLoader loader = loader(dependency);
             ImportedPipelineSources imported = new BlockDefinitionImporter(loader).importInto(application)) {
            var config = new PipelineTemplateConfigLoader().load(imported.configPath());

            assertTrue(imported.temporary());
            assertTrue(config.typeModel().contains("DocumentFile"));
            assertEquals("org.example.DocumentFile", config.typeModel().javaTypeBinding("DocumentFile").orElseThrow());
            assertTrue(config.pipelines().containsKey("org.example.documents/document-text-extraction"));
            assertEquals("org.example.documents/document-text-extraction",
                config.steps().getFirst().pipelineReference().orElseThrow());
            assertEquals("org.example.documents/document-text-extraction",
                imported.definitions().getFirst().qualifiedId());
            assertTrue(imported.definitions().getFirst().definitionFingerprint().startsWith("sha256:"));
        }
    }

    @Test void rejectsUndeclaredExternalAuthorityInsideImportedDefinition() throws Exception {
        Path dependency = dependency("query", "org.example.bad", "bad", "1.0.0", "bad-block", """
            version: 3
            types:
              Input: { fields: [[value, string]] }
              Output: { fields: [[value, string]] }
            pipelines:
              bad-block:
                input: Input
                output: Output
                steps:
                  - { name: Query, kind: query, input: Input, output: Output, using: external, operation: find }
            """);

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("undeclared requirement 'external'"));
        }
    }

    @Test void linksApplicationBoundQueryAndCommandRequirementsIntoOrdinaryV3Steps() throws Exception {
        Path dependency = operationBlockDependency();
        Path application = application(operationBlockApplication("primary"));

        try (URLClassLoader loader = loader(dependency);
             ImportedPipelineSources imported = new BlockDefinitionImporter(loader).importInto(application)) {
            var yaml = new org.pipelineframework.config.pipeline.PipelineYamlConfigLoader().load(imported.configPath());
            var query = yaml.localPipelines().get("org.example.operations/lookup").getFirst();
            var command = yaml.localPipelines().get("org.example.operations/update").getFirst();

            assertEquals("primary", query.operationSelection().orElseThrow().using());
            assertEquals("primary", command.operationSelection().orElseThrow().using());
            assertEquals("com.example.EffectKeyCommandId", command.commandIdGenerator());
            assertEquals("RETURN_RECORDED", command.duplicatePolicy());
            assertEquals("AUTOMATED", command.operationSelection().orElseThrow().policy()
                .get("requiredExecutionPosture"));

            ImportedPipelineDefinition queryProvenance = imported.definitions().stream()
                .filter(definition -> definition.logicalName().equals("lookup"))
                .findFirst().orElseThrow();
            assertFalse(queryProvenance.definitionFingerprint().equals(
                queryProvenance.linkedDefinitionFingerprint()));
            var requirement = queryProvenance.resolvedRequirements().getFirst();
            assertEquals("graphql.read", requirement.name());
            assertEquals("QUERY", requirement.kind());
            assertEquals("primary", requirement.binding());
            assertEquals("acme.operations", requirement.provider());
            assertEquals(64, requirement.connectorConfigurationDigest().length());
        }
    }

    @Test void rejectsMissingUnknownAndExtraBlockRequirementMappings() throws Exception {
        Path dependency = operationBlockDependency();
        try (URLClassLoader loader = loader(dependency)) {
            var missing = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("primary").replace(
                        "  org.example.operations/lookup:\n    graphql.read:\n      using: primary\n", ""))));
            assertTrue(missing.getMessage().contains("requires application capability mappings"));

            var unknownBinding = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("missing"))));
            assertTrue(unknownBinding.getMessage().contains("unknown connector binding 'missing'"));

            var extra = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("primary").replace(
                        "    graphql.read:\n      using: primary\n",
                        "    graphql.read:\n      using: primary\n    extra.read:\n      using: primary\n"))));
            assertTrue(extra.getMessage().contains("unknown requirement 'extra.read'"));

            var queryWithCommandAuthority = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("primary").replace(
                        "    graphql.read:\n      using: primary\n",
                        "    graphql.read:\n      using: primary\n      duplicatePolicy: FAIL\n"))));
            assertTrue(queryWithCommandAuthority.getMessage().contains("unsupported field 'duplicatePolicy'"));

            var missingCommandAuthority = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("primary").replace(
                        "      commandIdGenerator: com.example.EffectKeyCommandId\n", ""))));
            assertTrue(missingCommandAuthority.getMessage().contains("requires commandIdGenerator"));

            var unknownDefinition = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("primary").replace(
                        "org.example.operations/lookup", "org.example.operations/missing"))));
            assertTrue(unknownDefinition.getMessage().contains("unknown qualified Block definition"),
                unknownDefinition.getMessage());
        }
    }

    @Test void keepsRequirementMatchingStableWhileRewritingMultipleCapabilities() throws Exception {
        Path dependency = multiRequirementBlockDependency();
        Path application = application("""
            version: 3
            basePackage: com.example
            connectors:
              z: { provider: acme.operations, version: 1, config: { connection: z-connection } }
              final: { provider: acme.operations, version: 1, config: { connection: final-connection } }
            blockBindings:
              org.example.multi/multi:
                a: { using: z }
                z: { using: final }
            types:
              LookupRequest: { java: com.example.LookupRequest, fields: [[key, string]] }
              LookupResult: { java: com.example.LookupResult, fields: [[value, string]] }
            steps: []
            """);

        try (URLClassLoader loader = loader(dependency);
             ImportedPipelineSources imported = new BlockDefinitionImporter(loader).importInto(application)) {
            var requirements = imported.definitions().getFirst().resolvedRequirements();
            assertEquals("a", requirements.get(0).name());
            assertEquals("first", requirements.get(0).operations().getFirst().id());
            assertEquals("z", requirements.get(1).name());
            assertEquals("second", requirements.get(1).operations().getFirst().id());
        }
    }

    @Test void rejectsRequirementKindMismatchAndUnusedRequirements() throws Exception {
        Path dependency = operationBlockDependency();
        Path manifest = dependency.resolve("META-INF/pipeline/blocks.json");
        Files.writeString(manifest, Files.readString(manifest).replace(
            "\"graphql.read\": { \"kind\": \"QUERY\" }",
            "\"graphql.read\": { \"kind\": \"COMMAND\" }"));
        try (URLClassLoader loader = loader(dependency)) {
            var mismatch = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(operationBlockApplication("primary"))));
            assertTrue(mismatch.getMessage().contains("requirement 'graphql.read' is COMMAND but is used by a QUERY"),
                mismatch.getMessage());
        }

        operationBlockDependency();
        Files.writeString(manifest, Files.readString(manifest).replace(
            "\"graphql.read\": { \"kind\": \"QUERY\" }",
            "\"graphql.read\": { \"kind\": \"QUERY\" }, \"unused.read\": { \"kind\": \"QUERY\" }"));
        try (URLClassLoader loader = loader(dependency)) {
            var unused = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(
                    operationBlockApplication("primary").replace(
                        "    graphql.read:\n      using: primary\n",
                        "    graphql.read:\n      using: primary\n    unused.read:\n      using: primary\n"))));
            assertTrue(unused.getMessage().contains("declares unused requirement 'unused.read'"));
        }
    }

    @Test void rejectsBlockBindingsWhenThePackageDependencyIsMissing() throws Exception {
        try (URLClassLoader loader = loader()) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(operationBlockApplication("primary"))));
            assertTrue(failure.getMessage().contains("Blocks that are not installed"), failure.getMessage());
        }
    }

    @Test void rejectsPackageOwnedCommandAuthority() throws Exception {
        Path dependency = operationBlockDependency();
        Path definition = dependency.resolve("META-INF/pipeline/definition.yaml");
        Files.writeString(definition, Files.readString(definition).replace(
            "using: graphql.write",
            "using: graphql.write\n        duplicatePolicy: FAIL"));
        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(operationBlockApplication("primary"))));
            assertTrue(failure.getMessage().contains("application-owned Command field 'duplicatePolicy'"));
        }
    }

    @Test void rejectsDelegatedExecutionInsideImportedDefinition() throws Exception {
        Path dependency = dependency("delegated", "org.example.bad", "bad", "1.0.0", "bad-block", """
            version: 3
            types:
              Input: { fields: [[value, string]] }
              Output: { fields: [[value, string]] }
            pipelines:
              bad-block:
                input: Input
                output: Output
                steps:
                  - { name: Delegate, kind: delegated, input: Input, output: Output }
            """);

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("forbidden step kind 'delegated'"));
        }
    }

    @Test void failsLocalAndImportedLogicalNameCollisionClearly() throws Exception {
        Path dependency = dependency("collision", "org.example", "example", "1.0.0", "normalize", block("normalize"));
        String application = minimalApplication() + """
            pipelines:
              normalize:
                input: LocalInput
                output: LocalOutput
                steps:
                  - { name: Local, service: org.example.LocalService, cardinality: ONE_TO_ONE, input: LocalInput, output: LocalOutput, java: { input: org.example.LocalInput, output: org.example.LocalOutput } }
            """;

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(application)));
            assertTrue(failure.getMessage().contains("collides with imported block"));
        }
    }

    @Test void failsLocalAndImportedQualifiedIdentityCollisionClearly() throws Exception {
        Path dependency = dependency("qualified-collision", "org.example", "example", "1.0.0",
            "normalize", block("normalize"));
        String application = minimalApplication() + """
            pipelines:
              org.example/normalize:
                input: LocalInput
                output: LocalOutput
                steps:
                  - { name: Local, service: org.example.LocalService, cardinality: ONE_TO_ONE, input: LocalInput, output: LocalOutput, java: { input: org.example.LocalInput, output: org.example.LocalOutput } }
            """;

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(application)));
            assertTrue(failure.getMessage().contains("Local pipeline definition 'org.example/normalize'"));
            assertTrue(failure.getMessage().contains("same qualified identity"));
        }
    }

    @Test void leavesTheApplicationUnchangedWhenNoBlockDependencyIsInstalled() throws Exception {
        Path application = application(minimalApplication());
        try (URLClassLoader loader = loader()) {
            ImportedPipelineSources imported = new BlockDefinitionImporter(loader).importInto(application);
            assertFalse(imported.temporary());
            assertEquals(application, imported.configPath());
            assertTrue(imported.definitions().isEmpty());
        }
    }

    @Test void rejectsAnAmbiguousImportedShortNameAtCompileTime() throws Exception {
        Path first = dependency("ambiguous-first", "org.example.first", "first", "1.0.0",
            "normalize", block("normalize"));
        Path second = dependency("ambiguous-second", "org.example.second", "second", "1.0.0",
            "normalize", block("normalize").replace("BlockInput", "SecondInput")
                .replace("BlockOutput", "SecondOutput"));
        String application = """
            version: 3
            appName: Consumer
            basePackage: org.example.consumer
            transport: LOCAL
            platform: COMPUTE
            contract: { input: LocalInput, output: LocalOutput }
            types:
              LocalInput: { fields: [[value, string]] }
              LocalOutput: { fields: [[value, string]] }
            steps:
              - { name: Normalize, pipeline: normalize, cardinality: ONE_TO_ONE, input: LocalInput, output: LocalOutput, java: { input: org.example.LocalInput, output: org.example.LocalOutput } }
            """;

        try (URLClassLoader loader = loader(first, second)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(application)));
            assertTrue(failure.getMessage().contains("Pipeline reference 'normalize' is ambiguous"));
            assertTrue(failure.getMessage().contains("org.example.first/normalize"));
            assertTrue(failure.getMessage().contains("org.example.second/normalize"));
        }
    }

    @Test void normalizesManifestNamespaceBeforeConstructingQualifiedIdentity() throws Exception {
        Path normalized = dependency("normalized", "  org.example.documents  ", "documents", "1.0.0",
            "normalize", block("normalize"));
        try (URLClassLoader loader = loader(normalized);
             ImportedPipelineSources imported = new BlockDefinitionImporter(loader)
                 .importInto(application(minimalApplication()))) {
            assertEquals("org.example.documents/normalize", imported.definitions().getFirst().qualifiedId());
        }
    }

    @Test void rejectsIdentitySeparatorInManifestNamespace() throws Exception {
        Path badNamespace = dependency("bad-namespace", "org.example/documents", "documents", "1.0.0",
            "normalize", block("normalize"));
        try (URLClassLoader loader = loader(badNamespace)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("namespace must not contain '/'"));
        }
    }

    @Test void rejectsIdentitySeparatorInManifestDefinitionName() throws Exception {
        Path badName = dependency("bad-name", "org.example.documents", "documents", "1.0.0",
            "nested/normalize", block("nested/normalize"));
        try (URLClassLoader loader = loader(badName)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("definition.name must not contain '/'"));
        }
    }

    @Test void rejectsDefinitionResourcePathOutsideThePackageRoot() throws Exception {
        Path dependency = dependency("path-traversal", "org.example.documents", "documents", "1.0.0",
            "normalize", block("normalize"));
        Path escapedDefinition = directory.resolve("outside.yaml");
        Files.writeString(escapedDefinition, block("normalize"));
        Path manifest = dependency.resolve("META-INF/pipeline/blocks.json");
        Files.writeString(manifest, Files.readString(manifest)
            .replace("META-INF/pipeline/definition.yaml", "../outside.yaml"));

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("must be package-relative"));
        }
    }

    @Test void rejectsDefinitionResourceSymlinkOutsideThePackageRoot() throws Exception {
        Path dependency = dependency("symlink-traversal", "org.example.documents", "documents", "1.0.0",
            "normalize", block("normalize"));
        Path escapedDefinition = directory.resolve("symlink-outside.yaml");
        Files.writeString(escapedDefinition, block("normalize"));
        Path linkedDefinition = dependency.resolve("linked-definition.yaml");
        try {
            Files.createSymbolicLink(linkedDefinition, escapedDefinition);
        } catch (UnsupportedOperationException | java.io.IOException exception) {
            Assumptions.assumeTrue(false, "Symbolic links are unavailable: " + exception.getMessage());
            return;
        }
        Path manifest = dependency.resolve("META-INF/pipeline/blocks.json");
        Files.writeString(manifest, Files.readString(manifest)
            .replace("META-INF/pipeline/definition.yaml", "linked-definition.yaml"));

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new BlockDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("after resolving symbolic links"));
        }
    }

    private Path dependency(String directoryName, String namespace, String artifact, String version,
                            String definition, String yaml) throws Exception {
        Path root = directory.resolve(directoryName);
        Path metadata = root.resolve("META-INF/pipeline");
        Files.createDirectories(metadata);
        Files.writeString(metadata.resolve("blocks.json"), """
            {
              "schemaVersion": 1,
              "namespace": "%s",
              "artifact": { "groupId": "org.example", "artifactId": "%s", "version": "%s" },
              "definitions": [{ "name": "%s", "resource": "META-INF/pipeline/definition.yaml" }]
            }
            """.formatted(namespace, artifact, version, definition));
        Files.writeString(metadata.resolve("definition.yaml"), yaml);
        return root;
    }

    private Path operationBlockDependency() throws Exception {
        Path root = directory.resolve("operation-block");
        Path metadata = root.resolve("META-INF/pipeline");
        Files.createDirectories(metadata);
        Files.writeString(metadata.resolve("blocks.json"), """
            {
              "schemaVersion": 1,
              "namespace": "org.example.operations",
              "artifact": { "groupId": "org.example", "artifactId": "operation-block", "version": "1.0.0" },
              "definitions": [
                { "name": "lookup", "resource": "META-INF/pipeline/definition.yaml",
                  "requires": { "graphql.read": { "kind": "QUERY" } } },
                { "name": "update", "resource": "META-INF/pipeline/definition.yaml",
                  "requires": { "graphql.write": { "kind": "COMMAND" } } }
              ]
            }
            """);
        Files.writeString(metadata.resolve("definition.yaml"), """
            version: 3
            types:
              LookupRequest: { java: com.example.LookupRequest, fields: [[key, string]] }
              LookupResult: { java: com.example.LookupResult, fields: [[value, string]] }
              UpdateRequest: { java: com.example.UpdateRequest, fields: [[effectKey, string]] }
              UpdateResult: { java: com.example.UpdateResult, fields: [[accepted, boolean]] }
            pipelines:
              lookup:
                input: LookupRequest
                output: LookupResult
                steps:
                  - name: Execute operation
                    kind: query
                    cardinality: ONE_TO_ONE
                    using: graphql.read
                    operation: lookup
                    operationVersion: 1
                    input: LookupRequest
                    output: LookupResult
                    java: { input: com.example.LookupRequest, output: com.example.LookupResult }
              update:
                input: UpdateRequest
                output: UpdateResult
                steps:
                  - name: Execute operation
                    kind: command
                    cardinality: ONE_TO_ONE
                    using: graphql.write
                    operation: update
                    operationVersion: 1
                    input: UpdateRequest
                    output: UpdateResult
                    java: { input: com.example.UpdateRequest, output: com.example.UpdateResult }
            """);
        Files.writeString(metadata.resolve("connector-providers.json"), """
            {"schemaVersion":4,"providers":[{"id":"acme.operations","version":{"major":1,"minor":0},
            "configurationSchema":{"id":"acme.operations.binding","version":1,"fields":[
            {"name":"connection","type":"CONNECTION_REF","required":true}]},
            "operations":[
            {"id":"lookup","kind":"tpf:query","majorVersion":1,"queryCardinality":"ONE_TO_ONE",
            "typeContract":{"input":"com.example.LookupRequest","output":"com.example.LookupResult"}},
            {"id":"update","kind":"tpf:command","majorVersion":1,
            "typeContract":{"input":"com.example.UpdateRequest","output":"com.example.UpdateResult"},
            "commandCapabilities":{"retryRedriveSupported":false,"providerIdempotencySupported":false,
            "reconciliationSupported":false,"executionPosture":"AUTOMATED",
            "maximumMachineConfirmation":"PROVIDER_ACKNOWLEDGED","userConfirmationSupported":false,
            "durableReferenceKinds":[]}}]}]}
            """);
        return root;
    }

    private Path multiRequirementBlockDependency() throws Exception {
        Path root = directory.resolve("multi-requirement-block");
        Path metadata = root.resolve("META-INF/pipeline");
        Files.createDirectories(metadata);
        Files.writeString(metadata.resolve("blocks.json"), """
            {"schemaVersion":1,"namespace":"org.example.multi",
             "artifact":{"groupId":"org.example","artifactId":"multi","version":"1.0.0"},
             "definitions":[{"name":"multi","resource":"META-INF/pipeline/definition.yaml",
               "requires":{"a":{"kind":"QUERY"},"z":{"kind":"QUERY"}}}]}
            """);
        Files.writeString(metadata.resolve("definition.yaml"), """
            version: 3
            pipelines:
              multi:
                input: LookupRequest
                output: LookupResult
                steps:
                  - { name: First, kind: query, using: a, operation: first, input: LookupRequest, output: LookupResult, java: { input: com.example.LookupRequest, output: com.example.LookupResult } }
                  - { name: Second, kind: query, using: z, operation: second, input: LookupRequest, output: LookupResult, java: { input: com.example.LookupRequest, output: com.example.LookupResult } }
            """);
        Files.writeString(metadata.resolve("connector-providers.json"), """
            {"schemaVersion":4,"providers":[{"id":"acme.operations","version":{"major":1,"minor":0},
             "configurationSchema":{"id":"acme.operations.binding","version":1,"fields":[{"name":"connection","type":"CONNECTION_REF","required":true}]},
             "operations":[
               {"id":"first","kind":"tpf:query","majorVersion":1,"queryCardinality":"ONE_TO_ONE","typeContract":{"input":"com.example.LookupRequest","output":"com.example.LookupResult"}},
               {"id":"second","kind":"tpf:query","majorVersion":1,"queryCardinality":"ONE_TO_ONE","typeContract":{"input":"com.example.LookupRequest","output":"com.example.LookupResult"}}]}]}
            """);
        return root;
    }

    private static String operationBlockApplication(String binding) {
        return """
            version: 3
            appName: Consumer
            basePackage: com.example
            transport: LOCAL
            platform: COMPUTE
            contract: { input: LookupRequest, output: LookupResult }
            connectors:
              primary:
                provider: acme.operations
                version: 1
                config: { connection: primary-connection }
            blockBindings:
              org.example.operations/lookup:
                graphql.read:
                  using: %s
              org.example.operations/update:
                graphql.write:
                  using: %s
                  commandIdGenerator: com.example.EffectKeyCommandId
                  duplicatePolicy: RETURN_RECORDED
                  policy:
                    requiredExecutionPosture: AUTOMATED
                    minimumMachineConfirmation: PROVIDER_ACKNOWLEDGED
            steps:
              - name: Lookup
                pipeline: lookup
                cardinality: ONE_TO_ONE
                input: LookupRequest
                output: LookupResult
                java: { input: com.example.LookupRequest, output: com.example.LookupResult }
            """.formatted(binding, binding);
    }

    private Path application(String yaml) throws Exception {
        Path path = Files.createTempFile(directory, "application-", ".yaml");
        Files.writeString(path, yaml);
        return path;
    }

    private URLClassLoader loader(Path... roots) throws Exception {
        return new URLClassLoader(
            java.util.Arrays.stream(roots).map(root -> {
                try {
                    return root.toUri().toURL();
                } catch (java.net.MalformedURLException exception) {
                    throw new IllegalStateException(exception);
                }
            }).toArray(java.net.URL[]::new), null);
    }

    private static String minimalApplication() {
        return """
            version: 3
            appName: Consumer
            basePackage: org.example.consumer
            transport: LOCAL
            platform: COMPUTE
            contract: { input: LocalInput, output: LocalOutput }
            types:
              LocalInput: { fields: [[value, string]] }
              LocalOutput: { fields: [[value, string]] }
            steps:
              - { name: Local, service: org.example.LocalService, cardinality: ONE_TO_ONE, input: LocalInput, output: LocalOutput, java: { input: org.example.LocalInput, output: org.example.LocalOutput } }
            """;
    }

    private static String block(String name) {
        return """
            version: 3
            types:
              BlockInput: { fields: [[value, string]] }
              BlockOutput: { fields: [[value, string]] }
            pipelines:
              %s:
                input: BlockInput
                output: BlockOutput
                steps:
                  - { name: Block, service: org.example.BlockService, cardinality: ONE_TO_ONE, input: BlockInput, output: BlockOutput, java: { input: org.example.BlockInput, output: org.example.BlockOutput } }
            """.formatted(name);
    }
}
