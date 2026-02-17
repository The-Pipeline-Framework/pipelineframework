package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Aprotobuf.descriptor.file=" + resourcePath("descriptor_set_search.dsc"))
            .compile(
                JavaFileObjects.forSourceString(
                    "org.example.OrchestratorMarker",
                    """
                        package org.example;

                        import org.pipelineframework.annotation.PipelineOrchestrator;

                        @PipelineOrchestrator
                        public class OrchestratorMarker {
                        }
                        """),
                searchServiceStub("CrawlSourceService", "CrawlRequest", "RawDocument"),
                searchServiceStub("ParseDocumentService", "RawDocument", "ParsedDocument"),
                searchServiceStub("TokenizeContentService", "ParsedDocument", "TokenBatch"),
                searchServiceStub("IndexDocumentService", "TokenBatch", "IndexAck"),
                // Domain type stubs
                domainStub("CrawlRequest"),
                domainStub("RawDocument"),
                domainStub("ParsedDocument"),
                domainStub("TokenBatch"),
                domainStub("IndexAck"),
                dtoStub("CrawlRequestDto"),
                dtoStub("RawDocumentDto"),
                dtoStub("ParsedDocumentDto"),
                dtoStub("TokenBatchDto"),
                dtoStub("IndexAckDto"),
                mapperStub("CrawlRequestMapper", "CrawlRequest", "CrawlRequestDto"),
                mapperStub("RawDocumentMapper", "RawDocument", "RawDocumentDto"),
                mapperStub("ParsedDocumentMapper", "ParsedDocument", "ParsedDocumentDto"),
                mapperStub("TokenBatchMapper", "TokenBatch", "TokenBatchDto"),
                mapperStub("IndexAckMapper", "IndexAck", "IndexAckDto"));

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

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions("-Apipeline.generatedSourcesDir=" + generatedSourcesDir)
            .compile(
                JavaFileObjects.forSourceString(
                    "org.example.OrchestratorMarker",
                    """
                        package org.example;

                        import org.pipelineframework.annotation.PipelineOrchestrator;

                        @PipelineOrchestrator
                        public class OrchestratorMarker {
                        }
                        """),
                searchServiceStub("CrawlSourceService", "CrawlRequest", "RawDocument"),
                searchServiceStub("ParseDocumentService", "RawDocument", "ParsedDocument"),
                searchServiceStub("TokenizeContentService", "ParsedDocument", "TokenBatch"),
                searchServiceStub("IndexDocumentService", "TokenBatch", "IndexAck"),
                domainStub("CrawlRequest"),
                domainStub("RawDocument"),
                domainStub("ParsedDocument"),
                domainStub("TokenBatch"),
                domainStub("IndexAck"),
                dtoStub("CrawlRequestDto"),
                dtoStub("RawDocumentDto"),
                dtoStub("ParsedDocumentDto"),
                dtoStub("TokenBatchDto"),
                dtoStub("IndexAckDto"),
                mapperStub("CrawlRequestMapper", "CrawlRequest", "CrawlRequestDto"),
                mapperStub("RawDocumentMapper", "RawDocument", "RawDocumentDto"),
                mapperStub("ParsedDocumentMapper", "ParsedDocument", "ParsedDocumentDto"),
                mapperStub("TokenBatchMapper", "TokenBatch", "TokenBatchDto"),
                mapperStub("IndexAckMapper", "IndexAck", "IndexAckDto"));

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

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Aprotobuf.descriptor.file=" + resourcePath("descriptor_set_search.dsc"))
            .compile(
                JavaFileObjects.forSourceString(
                    "org.example.OrchestratorMarker",
                    """
                        package org.example;

                        import org.pipelineframework.annotation.PipelineOrchestrator;

                        @PipelineOrchestrator
                        public class OrchestratorMarker {
                        }
                        """),
                searchServiceStub("CrawlSourceService", "CrawlRequest", "RawDocument"),
                searchServiceStub("ParseDocumentService", "RawDocument", "ParsedDocument"),
                searchServiceStub("TokenizeContentService", "ParsedDocument", "TokenBatch"),
                searchServiceStub("IndexDocumentService", "TokenBatch", "IndexAck"),
                domainStub("CrawlRequest"),
                domainStub("RawDocument"),
                domainStub("ParsedDocument"),
                domainStub("TokenBatch"),
                domainStub("IndexAck"),
                dtoStub("CrawlRequestDto"),
                dtoStub("RawDocumentDto"),
                dtoStub("ParsedDocumentDto"),
                dtoStub("TokenBatchDto"),
                dtoStub("IndexAckDto"),
                mapperStub("CrawlRequestMapper", "CrawlRequest", "CrawlRequestDto"),
                mapperStub("RawDocumentMapper", "RawDocument", "RawDocumentDto"),
                mapperStub("ParsedDocumentMapper", "ParsedDocument", "ParsedDocumentDto"),
                mapperStub("TokenBatchMapper", "TokenBatch", "TokenBatchDto"),
                mapperStub("IndexAckMapper", "IndexAck", "IndexAckDto"));

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

    private Path resourcePath(String name) {
        return Paths.get(System.getProperty("user.dir"))
            .resolve("src/test/resources")
            .resolve(name);
    }

    private String stripAspectsSection(String yaml) {
        int index = yaml.indexOf("\naspects:");
        if (index < 0) {
            return yaml;
        }
        return yaml.substring(0, index + 1);
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

    private JavaFileObject dtoStub(String className) {
        String fqcn = "org.pipelineframework.search.dto." + className;
        String source = """
            package org.pipelineframework.search.dto;

            public class %s {
            }
            """.formatted(className);
        return JavaFileObjects.forSourceString(fqcn, source);
    }

    private JavaFileObject mapperStub(String mapperName, String domainType, String dtoType) {
        String fqcn = "org.pipelineframework.search.mapper." + mapperName;
        String source = """
            package org.pipelineframework.search.mapper;

            import org.pipelineframework.search.domain.%s;
            import org.pipelineframework.search.dto.%s;

            public class %s {
                public %s fromDto(%s dto) {
                    return new %s();
                }

                public %s toDto(%s domain) {
                    return new %s();
                }
            }
            """.formatted(domainType, dtoType, mapperName, domainType, dtoType, domainType, dtoType, domainType, dtoType);
        return JavaFileObjects.forSourceString(fqcn, source);
    }

    private JavaFileObject searchServiceStub(String serviceName, String inputType, String outputType) {
        String fqcn = "org.pipelineframework.search." + serviceName;
        String source = """
            package org.pipelineframework.search;

            import org.pipelineframework.annotation.PipelineStep;
            import org.pipelineframework.service.ReactiveService;
            import org.pipelineframework.step.StepOneToOne;
            import io.smallrye.mutiny.Uni;
            import jakarta.enterprise.context.ApplicationScoped;

            @PipelineStep(
                inputType = org.pipelineframework.search.domain.%s.class,
                outputType = org.pipelineframework.search.domain.%s.class,
                stepType = StepOneToOne.class,
                inboundMapper = org.pipelineframework.search.mapper.%sMapper.class,
                outboundMapper = org.pipelineframework.search.mapper.%sMapper.class
            )
            @ApplicationScoped
            public class %s implements ReactiveService<org.pipelineframework.search.domain.%s, org.pipelineframework.search.domain.%s> {
                @Override
                public Uni<org.pipelineframework.search.domain.%s> process(org.pipelineframework.search.domain.%s input) {
                    return null;
                }
            }
            """.formatted(inputType, outputType, inputType, outputType, serviceName, inputType, outputType, outputType, inputType);
        return JavaFileObjects.forSourceString(fqcn, source);
    }

    private JavaFileObject domainStub(String className) {
        String fqcn = "org.pipelineframework.search.domain." + className;
        String source = """
            package org.pipelineframework.search.domain;

            public class %s {
            }
            """.formatted(className);
        return JavaFileObjects.forSourceString(fqcn, source);
    }
}
