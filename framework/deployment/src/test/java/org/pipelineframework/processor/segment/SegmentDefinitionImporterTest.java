package org.pipelineframework.processor.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;

class SegmentDefinitionImporterTest {
    @TempDir Path directory;

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
             ImportedPipelineSources imported = new SegmentDefinitionImporter(loader).importInto(application)) {
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

    @Test void rejectsExternalAuthorityInsideImportedDefinition() throws Exception {
        Path dependency = dependency("query", "org.example.bad", "bad", "1.0.0", "bad-segment", """
            version: 3
            types:
              Input: { fields: [[value, string]] }
              Output: { fields: [[value, string]] }
            pipelines:
              bad-segment:
                input: Input
                output: Output
                steps:
                  - { name: Query, kind: query, input: Input, output: Output, using: external, operation: find }
            """);

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new SegmentDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("forbidden step kind 'query'"));
        }
    }

    @Test void rejectsDelegatedExecutionInsideImportedDefinition() throws Exception {
        Path dependency = dependency("delegated", "org.example.bad", "bad", "1.0.0", "bad-segment", """
            version: 3
            types:
              Input: { fields: [[value, string]] }
              Output: { fields: [[value, string]] }
            pipelines:
              bad-segment:
                input: Input
                output: Output
                steps:
                  - { name: Delegate, kind: delegated, input: Input, output: Output }
            """);

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalStateException.class,
                () -> new SegmentDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("forbidden step kind 'delegated'"));
        }
    }

    @Test void failsLocalAndImportedLogicalNameCollisionClearly() throws Exception {
        Path dependency = dependency("collision", "org.example", "example", "1.0.0", "normalize", segment("normalize"));
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
                () -> new SegmentDefinitionImporter(loader).importInto(application(application)));
            assertTrue(failure.getMessage().contains("collides with imported segment"));
        }
    }

    @Test void failsLocalAndImportedQualifiedIdentityCollisionClearly() throws Exception {
        Path dependency = dependency("qualified-collision", "org.example", "example", "1.0.0",
            "normalize", segment("normalize"));
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
                () -> new SegmentDefinitionImporter(loader).importInto(application(application)));
            assertTrue(failure.getMessage().contains("Local pipeline definition 'org.example/normalize'"));
            assertTrue(failure.getMessage().contains("same qualified identity"));
        }
    }

    @Test void leavesTheApplicationUnchangedWhenNoSegmentDependencyIsInstalled() throws Exception {
        Path application = application(minimalApplication());
        try (URLClassLoader loader = loader()) {
            ImportedPipelineSources imported = new SegmentDefinitionImporter(loader).importInto(application);
            assertFalse(imported.temporary());
            assertEquals(application, imported.configPath());
            assertTrue(imported.definitions().isEmpty());
        }
    }

    @Test void rejectsAnAmbiguousImportedShortNameAtCompileTime() throws Exception {
        Path first = dependency("ambiguous-first", "org.example.first", "first", "1.0.0",
            "normalize", segment("normalize"));
        Path second = dependency("ambiguous-second", "org.example.second", "second", "1.0.0",
            "normalize", segment("normalize").replace("SegmentInput", "SecondInput")
                .replace("SegmentOutput", "SecondOutput"));
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
                () -> new SegmentDefinitionImporter(loader).importInto(application(application)));
            assertTrue(failure.getMessage().contains("Pipeline reference 'normalize' is ambiguous"));
            assertTrue(failure.getMessage().contains("org.example.first/normalize"));
            assertTrue(failure.getMessage().contains("org.example.second/normalize"));
        }
    }

    @Test void normalizesManifestNamespaceBeforeConstructingQualifiedIdentity() throws Exception {
        Path normalized = dependency("normalized", "  org.example.documents  ", "documents", "1.0.0",
            "normalize", segment("normalize"));
        try (URLClassLoader loader = loader(normalized);
             ImportedPipelineSources imported = new SegmentDefinitionImporter(loader)
                 .importInto(application(minimalApplication()))) {
            assertEquals("org.example.documents/normalize", imported.definitions().getFirst().qualifiedId());
        }
    }

    @Test void rejectsIdentitySeparatorInManifestNamespace() throws Exception {
        Path badNamespace = dependency("bad-namespace", "org.example/documents", "documents", "1.0.0",
            "normalize", segment("normalize"));
        try (URLClassLoader loader = loader(badNamespace)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("namespace must not contain '/'"));
        }
    }

    @Test void rejectsIdentitySeparatorInManifestDefinitionName() throws Exception {
        Path badName = dependency("bad-name", "org.example.documents", "documents", "1.0.0",
            "nested/normalize", segment("nested/normalize"));
        try (URLClassLoader loader = loader(badName)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("definition.name must not contain '/'"));
        }
    }

    @Test void rejectsDefinitionResourcePathOutsideThePackageRoot() throws Exception {
        Path dependency = dependency("path-traversal", "org.example.documents", "documents", "1.0.0",
            "normalize", segment("normalize"));
        Path escapedDefinition = directory.resolve("outside.yaml");
        Files.writeString(escapedDefinition, segment("normalize"));
        Path manifest = dependency.resolve("META-INF/pipeline/segments.json");
        Files.writeString(manifest, Files.readString(manifest)
            .replace("META-INF/pipeline/definition.yaml", "../outside.yaml"));

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("must be package-relative"));
        }
    }

    @Test void rejectsDefinitionResourceSymlinkOutsideThePackageRoot() throws Exception {
        Path dependency = dependency("symlink-traversal", "org.example.documents", "documents", "1.0.0",
            "normalize", segment("normalize"));
        Path escapedDefinition = directory.resolve("symlink-outside.yaml");
        Files.writeString(escapedDefinition, segment("normalize"));
        Path linkedDefinition = dependency.resolve("linked-definition.yaml");
        try {
            Files.createSymbolicLink(linkedDefinition, escapedDefinition);
        } catch (UnsupportedOperationException | java.io.IOException exception) {
            Assumptions.assumeTrue(false, "Symbolic links are unavailable: " + exception.getMessage());
            return;
        }
        Path manifest = dependency.resolve("META-INF/pipeline/segments.json");
        Files.writeString(manifest, Files.readString(manifest)
            .replace("META-INF/pipeline/definition.yaml", "linked-definition.yaml"));

        try (URLClassLoader loader = loader(dependency)) {
            var failure = assertThrows(IllegalArgumentException.class,
                () -> new SegmentDefinitionImporter(loader).importInto(application(minimalApplication())));
            assertTrue(failure.getMessage().contains("after resolving symbolic links"));
        }
    }

    private Path dependency(String directoryName, String namespace, String artifact, String version,
                            String definition, String yaml) throws Exception {
        Path root = directory.resolve(directoryName);
        Path metadata = root.resolve("META-INF/pipeline");
        Files.createDirectories(metadata);
        Files.writeString(metadata.resolve("segments.json"), """
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

    private static String segment(String name) {
        return """
            version: 3
            types:
              SegmentInput: { fields: [[value, string]] }
              SegmentOutput: { fields: [[value, string]] }
            pipelines:
              %s:
                input: SegmentInput
                output: SegmentOutput
                steps:
                  - { name: Segment, service: org.example.SegmentService, cardinality: ONE_TO_ONE, input: SegmentInput, output: SegmentOutput, java: { input: org.example.SegmentInput, output: org.example.SegmentOutput } }
            """.formatted(name);
    }
}
