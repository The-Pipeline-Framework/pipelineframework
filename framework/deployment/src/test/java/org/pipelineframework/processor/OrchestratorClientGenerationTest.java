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
                // Simple mapper stubs with proper custom types
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.CrawlRequest", "package org.pipelineframework.search.common.domain; public class CrawlRequest {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.RawDocument", "package org.pipelineframework.search.common.domain; public class RawDocument {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.ParsedDocument", "package org.pipelineframework.search.common.domain; public class ParsedDocument {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.TokenBatch", "package org.pipelineframework.search.common.domain; public class TokenBatch {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.IndexAck", "package org.pipelineframework.search.common.domain; public class IndexAck {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.CrawlRequestDto", "package org.pipelineframework.search.common.dto; public class CrawlRequestDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.RawDocumentDto", "package org.pipelineframework.search.common.dto; public class RawDocumentDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.ParsedDocumentDto", "package org.pipelineframework.search.common.dto; public class ParsedDocumentDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.TokenBatchDto", "package org.pipelineframework.search.common.dto; public class TokenBatchDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.IndexAckDto", "package org.pipelineframework.search.common.dto; public class IndexAckDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.CrawlRequestGrpcMessage", "package org.pipelineframework.search.common.domain; public class CrawlRequestGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.RawDocumentGrpcMessage", "package org.pipelineframework.search.common.domain; public class RawDocumentGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.ParsedDocumentGrpcMessage", "package org.pipelineframework.search.common.domain; public class ParsedDocumentGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.TokenBatchGrpcMessage", "package org.pipelineframework.search.common.domain; public class TokenBatchGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.IndexAckGrpcMessage", "package org.pipelineframework.search.common.domain; public class IndexAckGrpcMessage {}"),
                // Mapper stubs
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.CrawlRequestMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.CrawlRequest;
                    import org.pipelineframework.search.common.dto.CrawlRequestDto;
                    import org.pipelineframework.search.common.domain.CrawlRequestGrpcMessage;
                    public class CrawlRequestMapper implements Mapper<CrawlRequestGrpcMessage, CrawlRequestDto, CrawlRequest> {
                        public CrawlRequestDto fromGrpc(CrawlRequestGrpcMessage grpc) { return null; }
                        public CrawlRequestGrpcMessage toGrpc(CrawlRequestDto dto) { return null; }
                        public CrawlRequest fromDto(CrawlRequestDto dto) { return null; }
                        public CrawlRequestDto toDto(CrawlRequest domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.RawDocumentMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.RawDocument;
                    import org.pipelineframework.search.common.dto.RawDocumentDto;
                    import org.pipelineframework.search.common.domain.RawDocumentGrpcMessage;
                    public class RawDocumentMapper implements Mapper<RawDocumentGrpcMessage, RawDocumentDto, RawDocument> {
                        public RawDocumentDto fromGrpc(RawDocumentGrpcMessage grpc) { return null; }
                        public RawDocumentGrpcMessage toGrpc(RawDocumentDto dto) { return null; }
                        public RawDocument fromDto(RawDocumentDto dto) { return null; }
                        public RawDocumentDto toDto(RawDocument domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.ParsedDocumentMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.ParsedDocument;
                    import org.pipelineframework.search.common.dto.ParsedDocumentDto;
                    import org.pipelineframework.search.common.domain.ParsedDocumentGrpcMessage;
                    public class ParsedDocumentMapper implements Mapper<ParsedDocumentGrpcMessage, ParsedDocumentDto, ParsedDocument> {
                        public ParsedDocumentDto fromGrpc(ParsedDocumentGrpcMessage grpc) { return null; }
                        public ParsedDocumentGrpcMessage toGrpc(ParsedDocumentDto dto) { return null; }
                        public ParsedDocument fromDto(ParsedDocumentDto dto) { return null; }
                        public ParsedDocumentDto toDto(ParsedDocument domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.TokenBatchMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.TokenBatch;
                    import org.pipelineframework.search.common.dto.TokenBatchDto;
                    import org.pipelineframework.search.common.domain.TokenBatchGrpcMessage;
                    public class TokenBatchMapper implements Mapper<TokenBatchGrpcMessage, TokenBatchDto, TokenBatch> {
                        public TokenBatchDto fromGrpc(TokenBatchGrpcMessage grpc) { return null; }
                        public TokenBatchGrpcMessage toGrpc(TokenBatchDto dto) { return null; }
                        public TokenBatch fromDto(TokenBatchDto dto) { return null; }
                        public TokenBatchDto toDto(TokenBatch domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.IndexAckMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.IndexAck;
                    import org.pipelineframework.search.common.dto.IndexAckDto;
                    import org.pipelineframework.search.common.domain.IndexAckGrpcMessage;
                    public class IndexAckMapper implements Mapper<IndexAckGrpcMessage, IndexAckDto, IndexAck> {
                        public IndexAckDto fromGrpc(IndexAckGrpcMessage grpc) { return null; }
                        public IndexAckGrpcMessage toGrpc(IndexAckDto dto) { return null; }
                        public IndexAck fromDto(IndexAckDto dto) { return null; }
                        public IndexAckDto toDto(IndexAck domain) { return null; }
                    }
                    """)
            );

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
                // Simple mapper stubs with proper custom types
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.CrawlRequest", "package org.pipelineframework.search.common.domain; public class CrawlRequest {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.RawDocument", "package org.pipelineframework.search.common.domain; public class RawDocument {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.ParsedDocument", "package org.pipelineframework.search.common.domain; public class ParsedDocument {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.TokenBatch", "package org.pipelineframework.search.common.domain; public class TokenBatch {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.IndexAck", "package org.pipelineframework.search.common.domain; public class IndexAck {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.CrawlRequestDto", "package org.pipelineframework.search.common.dto; public class CrawlRequestDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.RawDocumentDto", "package org.pipelineframework.search.common.dto; public class RawDocumentDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.ParsedDocumentDto", "package org.pipelineframework.search.common.dto; public class ParsedDocumentDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.TokenBatchDto", "package org.pipelineframework.search.common.dto; public class TokenBatchDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.IndexAckDto", "package org.pipelineframework.search.common.dto; public class IndexAckDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.CrawlRequestGrpcMessage", "package org.pipelineframework.search.common.domain; public class CrawlRequestGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.RawDocumentGrpcMessage", "package org.pipelineframework.search.common.domain; public class RawDocumentGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.ParsedDocumentGrpcMessage", "package org.pipelineframework.search.common.domain; public class ParsedDocumentGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.TokenBatchGrpcMessage", "package org.pipelineframework.search.common.domain; public class TokenBatchGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.IndexAckGrpcMessage", "package org.pipelineframework.search.common.domain; public class IndexAckGrpcMessage {}"),
                // Mapper stubs
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.CrawlRequestMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.CrawlRequest;
                    import org.pipelineframework.search.common.dto.CrawlRequestDto;
                    import org.pipelineframework.search.common.domain.CrawlRequestGrpcMessage;
                    public class CrawlRequestMapper implements Mapper<CrawlRequestGrpcMessage, CrawlRequestDto, CrawlRequest> {
                        public CrawlRequestDto fromGrpc(CrawlRequestGrpcMessage grpc) { return null; }
                        public CrawlRequestGrpcMessage toGrpc(CrawlRequestDto dto) { return null; }
                        public CrawlRequest fromDto(CrawlRequestDto dto) { return null; }
                        public CrawlRequestDto toDto(CrawlRequest domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.RawDocumentMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.RawDocument;
                    import org.pipelineframework.search.common.dto.RawDocumentDto;
                    import org.pipelineframework.search.common.domain.RawDocumentGrpcMessage;
                    public class RawDocumentMapper implements Mapper<RawDocumentGrpcMessage, RawDocumentDto, RawDocument> {
                        public RawDocumentDto fromGrpc(RawDocumentGrpcMessage grpc) { return null; }
                        public RawDocumentGrpcMessage toGrpc(RawDocumentDto dto) { return null; }
                        public RawDocument fromDto(RawDocumentDto dto) { return null; }
                        public RawDocumentDto toDto(RawDocument domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.ParsedDocumentMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.ParsedDocument;
                    import org.pipelineframework.search.common.dto.ParsedDocumentDto;
                    import org.pipelineframework.search.common.domain.ParsedDocumentGrpcMessage;
                    public class ParsedDocumentMapper implements Mapper<ParsedDocumentGrpcMessage, ParsedDocumentDto, ParsedDocument> {
                        public ParsedDocumentDto fromGrpc(ParsedDocumentGrpcMessage grpc) { return null; }
                        public ParsedDocumentGrpcMessage toGrpc(ParsedDocumentDto dto) { return null; }
                        public ParsedDocument fromDto(ParsedDocumentDto dto) { return null; }
                        public ParsedDocumentDto toDto(ParsedDocument domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.TokenBatchMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.TokenBatch;
                    import org.pipelineframework.search.common.dto.TokenBatchDto;
                    import org.pipelineframework.search.common.domain.TokenBatchGrpcMessage;
                    public class TokenBatchMapper implements Mapper<TokenBatchGrpcMessage, TokenBatchDto, TokenBatch> {
                        public TokenBatchDto fromGrpc(TokenBatchGrpcMessage grpc) { return null; }
                        public TokenBatchGrpcMessage toGrpc(TokenBatchDto dto) { return null; }
                        public TokenBatch fromDto(TokenBatchDto dto) { return null; }
                        public TokenBatchDto toDto(TokenBatch domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.IndexAckMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.IndexAck;
                    import org.pipelineframework.search.common.dto.IndexAckDto;
                    import org.pipelineframework.search.common.domain.IndexAckGrpcMessage;
                    public class IndexAckMapper implements Mapper<IndexAckGrpcMessage, IndexAckDto, IndexAck> {
                        public IndexAckDto fromGrpc(IndexAckGrpcMessage grpc) { return null; }
                        public IndexAckGrpcMessage toGrpc(IndexAckDto dto) { return null; }
                        public IndexAck fromDto(IndexAckDto dto) { return null; }
                        public IndexAckDto toDto(IndexAck domain) { return null; }
                    }
                    """)
            );

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
                // Simple mapper stubs with proper custom types
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.CrawlRequest", "package org.pipelineframework.search.common.domain; public class CrawlRequest {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.RawDocument", "package org.pipelineframework.search.common.domain; public class RawDocument {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.ParsedDocument", "package org.pipelineframework.search.common.domain; public class ParsedDocument {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.TokenBatch", "package org.pipelineframework.search.common.domain; public class TokenBatch {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.IndexAck", "package org.pipelineframework.search.common.domain; public class IndexAck {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.CrawlRequestDto", "package org.pipelineframework.search.common.dto; public class CrawlRequestDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.RawDocumentDto", "package org.pipelineframework.search.common.dto; public class RawDocumentDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.ParsedDocumentDto", "package org.pipelineframework.search.common.dto; public class ParsedDocumentDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.TokenBatchDto", "package org.pipelineframework.search.common.dto; public class TokenBatchDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.dto.IndexAckDto", "package org.pipelineframework.search.common.dto; public class IndexAckDto {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.CrawlRequestGrpcMessage", "package org.pipelineframework.search.common.domain; public class CrawlRequestGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.RawDocumentGrpcMessage", "package org.pipelineframework.search.common.domain; public class RawDocumentGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.ParsedDocumentGrpcMessage", "package org.pipelineframework.search.common.domain; public class ParsedDocumentGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.TokenBatchGrpcMessage", "package org.pipelineframework.search.common.domain; public class TokenBatchGrpcMessage {}"),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain.IndexAckGrpcMessage", "package org.pipelineframework.search.common.domain; public class IndexAckGrpcMessage {}"),
                // Mapper stubs
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.CrawlRequestMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.CrawlRequest;
                    import org.pipelineframework.search.common.dto.CrawlRequestDto;
                    import org.pipelineframework.search.common.domain.CrawlRequestGrpcMessage;
                    public class CrawlRequestMapper implements Mapper<CrawlRequestGrpcMessage, CrawlRequestDto, CrawlRequest> {
                        public CrawlRequestDto fromGrpc(CrawlRequestGrpcMessage grpc) { return null; }
                        public CrawlRequestGrpcMessage toGrpc(CrawlRequestDto dto) { return null; }
                        public CrawlRequest fromDto(CrawlRequestDto dto) { return null; }
                        public CrawlRequestDto toDto(CrawlRequest domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.RawDocumentMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.RawDocument;
                    import org.pipelineframework.search.common.dto.RawDocumentDto;
                    import org.pipelineframework.search.common.domain.RawDocumentGrpcMessage;
                    public class RawDocumentMapper implements Mapper<RawDocumentGrpcMessage, RawDocumentDto, RawDocument> {
                        public RawDocumentDto fromGrpc(RawDocumentGrpcMessage grpc) { return null; }
                        public RawDocumentGrpcMessage toGrpc(RawDocumentDto dto) { return null; }
                        public RawDocument fromDto(RawDocumentDto dto) { return null; }
                        public RawDocumentDto toDto(RawDocument domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.ParsedDocumentMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.ParsedDocument;
                    import org.pipelineframework.search.common.dto.ParsedDocumentDto;
                    import org.pipelineframework.search.common.domain.ParsedDocumentGrpcMessage;
                    public class ParsedDocumentMapper implements Mapper<ParsedDocumentGrpcMessage, ParsedDocumentDto, ParsedDocument> {
                        public ParsedDocumentDto fromGrpc(ParsedDocumentGrpcMessage grpc) { return null; }
                        public ParsedDocumentGrpcMessage toGrpc(ParsedDocumentDto dto) { return null; }
                        public ParsedDocument fromDto(ParsedDocumentDto dto) { return null; }
                        public ParsedDocumentDto toDto(ParsedDocument domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.TokenBatchMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.TokenBatch;
                    import org.pipelineframework.search.common.dto.TokenBatchDto;
                    import org.pipelineframework.search.common.domain.TokenBatchGrpcMessage;
                    public class TokenBatchMapper implements Mapper<TokenBatchGrpcMessage, TokenBatchDto, TokenBatch> {
                        public TokenBatchDto fromGrpc(TokenBatchGrpcMessage grpc) { return null; }
                        public TokenBatchGrpcMessage toGrpc(TokenBatchDto dto) { return null; }
                        public TokenBatch fromDto(TokenBatchDto dto) { return null; }
                        public TokenBatchDto toDto(TokenBatch domain) { return null; }
                    }
                    """),
                JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper.IndexAckMapper", """
                    package org.pipelineframework.search.common.mapper;
                    import org.pipelineframework.mapper.Mapper;
                    import org.pipelineframework.search.common.domain.IndexAck;
                    import org.pipelineframework.search.common.dto.IndexAckDto;
                    import org.pipelineframework.search.common.domain.IndexAckGrpcMessage;
                    public class IndexAckMapper implements Mapper<IndexAckGrpcMessage, IndexAckDto, IndexAck> {
                        public IndexAckDto fromGrpc(IndexAckGrpcMessage grpc) { return null; }
                        public IndexAckGrpcMessage toGrpc(IndexAckDto dto) { return null; }
                        public IndexAck fromDto(IndexAckDto dto) { return null; }
                        public IndexAckDto toDto(IndexAck domain) { return null; }
                    }
                    """)
            );

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
        String fqcn = "org.pipelineframework.search.common.dto." + className;
        String source = """
            package org.pipelineframework.search.common.dto;

            public class %s {
            }
            """.formatted(className);
        return JavaFileObjects.forSourceString(fqcn, source);
    }

    private JavaFileObject mapperStub(String mapperName, String grpcType, String dtoType, String domainType) {
        // Use abstract class instead of interface to avoid MapStruct processing issues in tests
        String source = String.format(
            "package org.pipelineframework.search.common.mapper;\n" +
            "\n" +
            "import org.pipelineframework.mapper.Mapper;\n" +
            "import org.pipelineframework.search.common.domain.%1$s;\n" +
            "import org.pipelineframework.search.common.dto.%2$s;\n" +
            "import org.pipelineframework.search.common.domain.%3$s;\n" +
            "\n" +
            "public abstract class %4$s implements Mapper<%1$sGrpcMessage, %2$s, %3$s> {\n" +
            "    public abstract %2$s fromGrpc(%1$sGrpcMessage grpc);\n" +
            "    public abstract %1$sGrpcMessage toGrpc(%2$s dto);\n" +
            "    public abstract %3$s fromDto(%2$s dto);\n" +
            "    public abstract %2$s toDto(%3$s domain);\n" +
            "}\n",
            grpcType, dtoType, domainType, mapperName
        );
        return JavaFileObjects.forSourceString("org.pipelineframework.search.common.mapper." + mapperName, source);
    }

    private JavaFileObject domainStub(String className) {
        String source = String.format(
            "package org.pipelineframework.search.common.domain;\n" +
            "\n" +
            "public class %s {\n" +
            "}\n",
            className
        );
        return JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain." + className, source);
    }

    private JavaFileObject grpcMessageStub(String className) {
        String source = String.format(
            "package org.pipelineframework.search.common.domain;\n" +
            "\n" +
            "public class %sGrpcMessage {\n" +
            "}\n",
            className
        );
        return JavaFileObjects.forSourceString("org.pipelineframework.search.common.domain." + className + "GrpcMessage", source);
    }
}
