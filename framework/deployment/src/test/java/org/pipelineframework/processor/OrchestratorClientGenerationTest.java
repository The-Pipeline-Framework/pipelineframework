package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestratorClientGenerationTest {

    @TempDir
    Path tempDir;

    /**
     * Common stub sources for all domain types used in orchestrator client generation tests.
     * Includes domain entities, DTOs, gRPC messages, and concrete mapper implementations.
     *
     * @return list of JavaFileObject stubs for CrawlRequest, RawDocument, ParsedDocument, TokenBatch, and IndexAck
     */
    private List<JavaFileObject> commonStubs() {
        List<JavaFileObject> stubs = new ArrayList<>();
        String[] types = {"CrawlRequest", "RawDocument", "ParsedDocument", "TokenBatch", "IndexAck"};

        for (String type : types) {
            stubs.add(domainStub(type));
            stubs.add(dtoStub(type));
            stubs.add(grpcMessageStub(type));
            stubs.add(mapperStub(type + "Mapper", type, type + "Dto", type));
        }

        return stubs;
    }

    @Test
    void generatesGrpcClientStepsFromTemplate() throws IOException {
        Path projectRoot = initProjectRoot();
        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target").resolve("generated-sources").resolve("pipeline");
        Files.createDirectories(generatedSourcesDir);

        Path pipelineYaml = projectRoot.resolve("pipeline.yaml");
        String pipelineConfig = Files.readString(resourcePath("pipeline-search.yaml"))
            .replace("transport: \"REST\"", "transport: \"GRPC\"");
        Files.writeString(pipelineYaml, stripAspectsSection(pipelineConfig));

        List<JavaFileObject> sources = new ArrayList<>();
        sources.add(orchestratorMarkerStub());
        sources.addAll(commonStubs());

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Aprotobuf.descriptor.file=" + resourcePath("descriptor_set_search.dsc"))
            .compile(sources.toArray(new JavaFileObject[0]));

        assertThat(compilation).succeeded();

        Path orchestratorClientDir = generatedSourcesDir.resolve("orchestrator-client");
        assertTrue(hasGeneratedClass(orchestratorClientDir, "ProcessCrawlSourceGrpcClientStep"));
        assertTrue(hasGeneratedClass(orchestratorClientDir, "ProcessIndexDocumentGrpcClientStep"));
    }

    @Test
    void generatesRestClientStepsFromTemplate() throws IOException {
        Path projectRoot = initProjectRoot();
        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target").resolve("generated-sources").resolve("pipeline");
        Files.createDirectories(generatedSourcesDir);

        Files.copy(resourcePath("pipeline-search.yaml"), projectRoot.resolve("pipeline.yaml"));

        List<JavaFileObject> sources = new ArrayList<>();
        sources.add(orchestratorMarkerStub());
        sources.addAll(commonStubs());

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions("-Apipeline.generatedSourcesDir=" + generatedSourcesDir)
            .compile(sources.toArray(new JavaFileObject[0]));

        assertThat(compilation).succeeded();

        Path orchestratorClientDir = generatedSourcesDir.resolve("orchestrator-client");
        assertTrue(hasGeneratedClass(orchestratorClientDir, "ProcessCrawlSourceRestClientStep"));
        assertTrue(hasGeneratedClass(orchestratorClientDir, "PersistenceRawDocumentSideEffectRestClientStep"));
    }

    @Test
    void generatesOrchestratorClientPropertiesWithModuleOverrides() throws IOException {
        Path projectRoot = initProjectRoot();
        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target").resolve("generated-sources").resolve("pipeline");
        Path moduleResourcesDir = moduleDir.resolve("src").resolve("main").resolve("resources");
        Files.createDirectories(generatedSourcesDir);
        Files.createDirectories(moduleResourcesDir);

        String moduleOverrides = """
            pipeline.module.search-svc.steps=process-crawl-source,process-parse-document
            pipeline.client.tls-configuration-name=pipeline-client
            """;
        Files.writeString(moduleResourcesDir.resolve("application.properties"), moduleOverrides);

        Path pipelineYaml = projectRoot.resolve("pipeline.yaml");
        String pipelineConfig = Files.readString(resourcePath("pipeline-search.yaml"))
            .replace("transport: \"REST\"", "transport: \"GRPC\"");
        Files.writeString(pipelineYaml, stripAspectsSection(pipelineConfig));

        List<JavaFileObject> sources = new ArrayList<>();
        sources.add(orchestratorMarkerStub());
        sources.addAll(commonStubs());

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Aprotobuf.descriptor.file=" + resourcePath("descriptor_set_search.dsc"))
            .compile(sources.toArray(new JavaFileObject[0]));

        assertThat(compilation).succeeded();

        JavaFileObject propertiesFile = compilation.generatedFile(
            StandardLocation.CLASS_OUTPUT,
            "META-INF/pipeline",
            "orchestrator-clients.properties").orElseThrow();
        String propertiesContent = propertiesFile.getCharContent(true).toString();

        assertTrue(propertiesContent.contains(
            "quarkus.grpc.clients.process-crawl-source.port=8444"));
        assertTrue(propertiesContent.contains(
            "quarkus.grpc.clients.process-parse-document.port=8444"));
        assertTrue(propertiesContent.contains(
            "tls-configuration-name=${pipeline.client.tls-configuration-name:pipeline-client}"));
    }

    private Path initProjectRoot() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <?xml version="1.0" encoding="UTF-8"?>
            <project xmlns="http://maven.apache.org/POM/4.0.0"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
                <modelVersion>4.0.0</modelVersion>
                <groupId>test</groupId>
                <artifactId>test-project</artifactId>
                <version>1.0.0</version>
                <packaging>pom</packaging>
            </project>
            """);
        return projectRoot;
    }

    private JavaFileObject orchestratorMarkerStub() {
        return JavaFileObjects.forSourceString(
            "org.example.OrchestratorMarker",
            """
                package org.example;

                import org.pipelineframework.annotation.PipelineOrchestrator;

                @PipelineOrchestrator
                public class OrchestratorMarker {
                }
                """);
    }

    private JavaFileObject domainStub(String className) {
        String source = String.format(
            "package org.pipelineframework.search.common.domain;\n\npublic class %s {\n}\n",
            className);
        return JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain." + className, source);
    }

    private JavaFileObject dtoStub(String className) {
        String fqcn = "org.pipelineframework.search.common.dto." + className;
        String source = """
            package org.pipelineframework.search.common.dto;

            public class %s {
            }
            """.formatted(className);
        return JavaFileObjects.forSourceString(fqcn, source);
    }

    private JavaFileObject mapperStub(String mapperName, String grpcType, String dtoType, String domainType) {
        // Concrete mapper class for test use (avoids MapStruct processing issues)
        String source = String.format(
            "package org.pipelineframework.search.common.mapper;\n" +
            "\n" +
            "import org.pipelineframework.mapper.Mapper;\n" +
            "import org.pipelineframework.search.common.domain.%1$s;\n" +
            "import org.pipelineframework.search.common.domain.%1$sGrpcMessage;\n" +
            "import org.pipelineframework.search.common.dto.%2$s;\n" +
            "import org.pipelineframework.search.common.domain.%3$s;\n" +
            "\n" +
            "public class %4$s implements Mapper<%1$sGrpcMessage, %2$s, %3$s> {\n" +
            "    public %2$s fromGrpc(%1$sGrpcMessage grpc) { return null; }\n" +
            "    public %1$sGrpcMessage toGrpc(%2$s dto) { return null; }\n" +
            "    public %3$s fromDto(%2$s dto) { return null; }\n" +
            "    public %2$s toDto(%3$s domain) { return null; }\n" +
            "}\n",
            grpcType, dtoType, domainType, mapperName
        );
        return JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper." + mapperName, source);
    }

    private JavaFileObject grpcMessageStub(String className) {
        String source = String.format(
            "package org.pipelineframework.search.common.domain;\n\npublic class %sGrpcMessage {\n}\n",
            className);
        return JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain." + className + "GrpcMessage", source);
    }

    private boolean hasGeneratedClass(Path rootDir, String className) throws IOException {
        if (!Files.exists(rootDir)) {
            return false;
        }
        try (var stream = Files.walk(rootDir)) {
            return stream.filter(path -> path.toString().endsWith(".java"))
                .anyMatch(path -> {
                    try {
                        return Files.readString(path).contains("class " + className);
                    } catch (IOException e) {
                        return false;
                    }
                });
        }
    }

    private Path resourcePath(String resourceName) {
        return Paths.get(System.getProperty("user.dir")).resolve("src/test/resources").resolve(resourceName);
    }

    private String stripAspectsSection(String yaml) {
        int aspectsIndex = yaml.indexOf("aspects:");
        return aspectsIndex >= 0 ? yaml.substring(0, aspectsIndex).trim() : yaml.trim();
    }
}
