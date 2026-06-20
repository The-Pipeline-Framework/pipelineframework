package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.URL;
import java.net.URISyntaxException;
import javax.tools.StandardLocation;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineGenerationPhaseIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesLocalClientArtifactsThroughProcessorPipeline() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>test</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);

        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);

        Files.writeString(projectRoot.resolve("pipeline.yaml"), """
            appName: "Test Pipeline"
            basePackage: "org.pipelineframework.search"
            transport: "LOCAL"
            steps:
              - name: "Process Crawl Source"
                service: "org.pipelineframework.search.service.ProcessCrawlSourceService"
                input: "org.pipelineframework.search.domain.CrawlRequest"
                output: "org.pipelineframework.search.domain.RawDocument"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString().replace('\\', '/'),
                "-Aprotobuf.descriptor.file="
                    + resourcePath("descriptor_set_search.dsc").toString().replace('\\', '/'))
            .compile(
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.service.ProcessCrawlSourceService",
                    """
                        package org.pipelineframework.search.service;

                        import io.smallrye.mutiny.Uni;
                        import org.pipelineframework.annotation.PipelineStep;
                        import org.pipelineframework.service.ReactiveService;
                        import org.pipelineframework.step.StepOneToOne;

                        @PipelineStep(
                            inputType = org.pipelineframework.search.domain.CrawlRequest.class,
                            outputType = org.pipelineframework.search.domain.RawDocument.class,
                            stepType = StepOneToOne.class,
                            inboundMapper = org.pipelineframework.search.mapper.CrawlRequestMapper.class,
                            outboundMapper = org.pipelineframework.search.mapper.RawDocumentMapper.class
                        )
                        public class ProcessCrawlSourceService implements ReactiveService<org.pipelineframework.search.domain.CrawlRequest, org.pipelineframework.search.domain.RawDocument> {
                            @Override
                            public Uni<org.pipelineframework.search.domain.RawDocument> process(org.pipelineframework.search.domain.CrawlRequest input) {
                                return Uni.createFrom().item(new org.pipelineframework.search.domain.RawDocument());
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.domain.CrawlRequest",
                    """
                        package org.pipelineframework.search.domain;

                        public class CrawlRequest {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.domain.RawDocument",
                    """
                        package org.pipelineframework.search.domain;

                        public class RawDocument {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.dto.CrawlRequestDto",
                    """
                        package org.pipelineframework.search.dto;

                        public class CrawlRequestDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.dto.RawDocumentDto",
                    """
                        package org.pipelineframework.search.dto;

                        public class RawDocumentDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.mapper.CrawlRequestMapper",
                    """
                        package org.pipelineframework.search.mapper;

                        import org.pipelineframework.mapper.Mapper;
                        import org.pipelineframework.search.domain.CrawlRequest;
                        import org.pipelineframework.search.dto.CrawlRequestDto;

                        public class CrawlRequestMapper implements Mapper<CrawlRequest, CrawlRequestDto> {
                            @Override
                            public CrawlRequest fromExternal(CrawlRequestDto dto) {
                                return new CrawlRequest();
                            }

                            @Override
                            public CrawlRequestDto toExternal(CrawlRequest domain) {
                                return new CrawlRequestDto();
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.mapper.RawDocumentMapper",
                    """
                        package org.pipelineframework.search.mapper;

                        import org.pipelineframework.mapper.Mapper;
                        import org.pipelineframework.search.domain.RawDocument;
                        import org.pipelineframework.search.dto.RawDocumentDto;

                        public class RawDocumentMapper implements Mapper<RawDocument, RawDocumentDto> {
                            @Override
                            public RawDocument fromExternal(RawDocumentDto dto) {
                                return new RawDocument();
                            }

                            @Override
                            public RawDocumentDto toExternal(RawDocument domain) {
                                return new RawDocumentDto();
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.mapper.Mapper",
                    """
                        package org.pipelineframework.mapper;

                        public interface Mapper<TDomain, TExternal> {
                            TDomain fromExternal(TExternal external);

                            TExternal toExternal(TDomain domain);
                        }
                        """));

        assertThat(compilation).succeeded();

        Path grpcServerDir = generatedSourcesDir.resolve("pipeline-server");
        assertFalse(hasGeneratedClass(grpcServerDir, "ProcessCrawlSourceGrpcService"));
        assertTrue(compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", "roles.json").isPresent());
    }

    @Test
    void springProfileGeneratesYamlOnlyLocalUnaryClientStep() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>spring-smoke</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);

        Path moduleDir = projectRoot.resolve("spring-smoke");
        Path generatedSourcesDir = moduleDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);

        Path pipelineConfig = projectRoot.resolve("pipeline.spring.yaml");
        Files.writeString(pipelineConfig, """
            appName: "Spring Smoke"
            basePackage: "com.example"
            transport: "LOCAL"
            platform: "COMPUTE"
            steps:
              - name: "payment"
                service: "com.example.service.PaymentService"
                input: "com.example.service.PaymentRecord"
                output: "com.example.service.PaymentStatus"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString().replace('\\', '/'),
                "-Apipeline.config=" + pipelineConfig.toString().replace('\\', '/'),
                "-Apipeline.transport=LOCAL",
                "-Apipeline.platform=COMPUTE",
                "-Apipeline.codegen.rendererProfile=spring")
            .compile(
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentService",
                    """
                        package com.example.service;

                        import io.smallrye.mutiny.Uni;
                        import org.pipelineframework.service.ReactiveService;

                        public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                            @Override
                            public Uni<PaymentStatus> process(PaymentRecord input) {
                                return Uni.createFrom().item(new PaymentStatus());
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentRecord",
                    """
                        package com.example.service;

                        public class PaymentRecord {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentStatus",
                    """
                        package com.example.service;

                        public class PaymentStatus {
                        }
                        """));

        assertThat(compilation).succeeded();

        Path springClientStep = findGeneratedClass(generatedSourcesDir, "ProcessPaymentLocalClientStep");
        assertTrue(
            Files.exists(springClientStep),
            "Expected Spring local client step source to be generated under "
                + generatedSourcesDir
                + "; found "
                + generatedJavaFiles(generatedSourcesDir));
        String source = Files.readString(springClientStep);
        assertTrue(source.contains("import org.pipelineframework.runtime.core.PipelineUnaryStep;"));
        assertTrue(source.contains("import org.springframework.stereotype.Component;"));
        assertFalse(source.contains("io.quarkus."));
        assertFalse(source.contains("jakarta.enterprise."));
        assertFalse(source.contains("io.vertx."));
        assertTrue(compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", "roles.json").isPresent());
    }

    @Test
    void springProfileGeneratesYamlOnlyRestUnaryResourceAndStepBean() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>spring-rest-smoke</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);

        Path moduleDir = projectRoot.resolve("spring-rest-smoke");
        Path generatedSourcesDir = moduleDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);

        Path pipelineConfig = projectRoot.resolve("pipeline.spring-rest.yaml");
        Files.writeString(pipelineConfig, """
            appName: "Spring REST Smoke"
            basePackage: "com.example"
            transport: "REST"
            platform: "COMPUTE"
            steps:
              - name: "payment"
                service: "com.example.service.PaymentService"
                input: "com.example.domain.PaymentRecord"
                inboundMapper: "com.example.mapper.PaymentRecordMapper"
                output: "com.example.domain.PaymentStatus"
                outboundMapper: "com.example.mapper.PaymentStatusMapper"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString().replace('\\', '/'),
                "-Apipeline.config=" + pipelineConfig.toString().replace('\\', '/'),
                "-Apipeline.transport=REST",
                "-Apipeline.platform=COMPUTE",
                "-Apipeline.codegen.rendererProfile=spring")
            .compile(
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentService",
                    """
                        package com.example.service;

                        import com.example.domain.PaymentRecord;
                        import com.example.domain.PaymentStatus;
                        import io.smallrye.mutiny.Uni;
                        import org.pipelineframework.service.ReactiveService;

                        public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                            @Override
                            public Uni<PaymentStatus> process(PaymentRecord input) {
                                return Uni.createFrom().item(new PaymentStatus());
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.domain.PaymentRecord",
                    """
                        package com.example.domain;

                        public class PaymentRecord {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.domain.PaymentStatus",
                    """
                        package com.example.domain;

                        public class PaymentStatus {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.dto.PaymentRecordDto",
                    """
                        package com.example.dto;

                        public class PaymentRecordDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.dto.PaymentStatusDto",
                    """
                        package com.example.dto;

                        public class PaymentStatusDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.mapper.PaymentRecordMapper",
                    """
                        package com.example.mapper;

                        import com.example.domain.PaymentRecord;
                        import com.example.dto.PaymentRecordDto;
                        import org.pipelineframework.mapper.Mapper;

                        public class PaymentRecordMapper implements Mapper<PaymentRecord, PaymentRecordDto> {
                            @Override
                            public PaymentRecord fromExternal(PaymentRecordDto external) {
                                return new PaymentRecord();
                            }

                            @Override
                            public PaymentRecordDto toExternal(PaymentRecord domain) {
                                return new PaymentRecordDto();
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.mapper.PaymentStatusMapper",
                    """
                        package com.example.mapper;

                        import com.example.domain.PaymentStatus;
                        import com.example.dto.PaymentStatusDto;
                        import org.pipelineframework.mapper.Mapper;

                        public class PaymentStatusMapper implements Mapper<PaymentStatus, PaymentStatusDto> {
                            @Override
                            public PaymentStatus fromExternal(PaymentStatusDto external) {
                                return new PaymentStatus();
                            }

                            @Override
                            public PaymentStatusDto toExternal(PaymentStatus domain) {
                                return new PaymentStatusDto();
                            }
                        }
                        """));

        assertThat(compilation).succeeded();

        Path springResource = findGeneratedClass(generatedSourcesDir, "ProcessPaymentResource");
        Path springClientStep = findGeneratedClass(generatedSourcesDir, "ProcessPaymentLocalClientStep");
        assertTrue(Files.exists(springResource), "Expected Spring REST resource in " + generatedJavaFiles(generatedSourcesDir));
        assertTrue(Files.exists(springClientStep), "Expected Spring unary step bean in " + generatedJavaFiles(generatedSourcesDir));

        String resourceSource = Files.readString(springResource);
        assertTrue(resourceSource.contains("import org.springframework.web.bind.annotation.RestController;"));
        assertTrue(resourceSource.contains("import reactor.core.publisher.Mono;"));
        assertFalse(resourceSource.contains("io.quarkus."));
        assertFalse(resourceSource.contains("jakarta.enterprise."));
        assertFalse(resourceSource.contains("io.vertx."));
        assertFalse(resourceSource.contains("org.jboss.resteasy."));
        assertFalse(resourceSource.contains("jakarta.ws.rs."));

        String clientStepSource = Files.readString(springClientStep);
        assertTrue(clientStepSource.contains("import org.springframework.stereotype.Component;"));
        assertTrue(clientStepSource.contains("import java.util.concurrent.CompletionStage;"));
        assertTrue(clientStepSource.contains("implements PipelineUnaryStep<PaymentRecord, PaymentStatus>"));
        assertFalse(clientStepSource.contains("io.quarkus."));
        assertFalse(clientStepSource.contains("jakarta.enterprise."));
        assertFalse(clientStepSource.contains("io.vertx."));
        assertFalse(clientStepSource.contains("org.jboss.resteasy."));
        assertFalse(clientStepSource.contains("jakarta.ws.rs."));
        assertTrue(compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", "roles.json").isPresent());
    }

    @Test
    void quarkusProfileUsesYamlVirtualThreadsForBlockingServiceBridge() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>quarkus-blocking</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);

        Path moduleDir = projectRoot.resolve("quarkus-blocking");
        Path generatedSourcesDir = moduleDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);

        Path pipelineConfig = projectRoot.resolve("pipeline.quarkus-blocking.yaml");
        Files.writeString(pipelineConfig, """
            appName: "Quarkus Blocking"
            basePackage: "com.example"
            transport: "LOCAL"
            platform: "COMPUTE"
            steps:
              - name: "payment"
                service: "com.example.service.PaymentService"
                input: "com.example.service.PaymentRecord"
                output: "com.example.service.PaymentStatus"
                runOnVirtualThreads: true
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString().replace('\\', '/'),
                "-Apipeline.config=" + pipelineConfig.toString().replace('\\', '/'),
                "-Apipeline.transport=LOCAL",
                "-Apipeline.platform=COMPUTE")
            .compile(
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentService",
                    """
                        package com.example.service;

                        import org.pipelineframework.service.blocking.BlockingService;

                        public class PaymentService implements BlockingService<PaymentRecord, PaymentStatus> {
                            @Override
                            public PaymentStatus processBlocking(PaymentRecord input) {
                                return new PaymentStatus();
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentRecord",
                    """
                        package com.example.service;

                        public class PaymentRecord {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.service.PaymentStatus",
                    """
                        package com.example.service;

                        public class PaymentStatus {
                        }
                        """));

        assertThat(compilation).succeeded();

        Path bridge = findGeneratedClass(generatedSourcesDir, "ProcessPaymentServiceBlockingReactiveBridge");
        assertTrue(Files.exists(bridge), "Expected Quarkus blocking bridge in " + generatedJavaFiles(generatedSourcesDir));
        String bridgeSource = Files.readString(bridge);
        assertTrue(bridgeSource.contains("import jakarta.enterprise.context.ApplicationScoped;"));
        assertTrue(bridgeSource.contains("blockingExecutionSupport.supply(true, () -> blockingService.processBlocking(processableObj))"));
    }

    @Test
    void generatesRestServerArtifactsForRuntimeMappedModularFunctionStepModule() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>test</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);

        Path configDir = projectRoot.resolve("config");
        Files.createDirectories(configDir);
        Files.writeString(configDir.resolve("pipeline.modular-lambda.yaml"), """
            appName: "Search Pipeline"
            basePackage: "org.pipelineframework.search"
            transport: "REST"
            platform: "FUNCTION"
            steps:
              - name: "Crawl Source"
                service: "org.pipelineframework.search.crawl_source.service.ProcessCrawlSourceService"
                input: "org.pipelineframework.search.common.domain.CrawlRequest"
                inputTypeName: "CrawlRequest"
                inboundMapper: "org.pipelineframework.search.crawl_source.mapper.CrawlRequestMapper"
                output: "org.pipelineframework.search.common.domain.RawDocument"
                outputTypeName: "RawDocument"
                outboundMapper: "org.pipelineframework.search.crawl_source.mapper.RawDocumentMapper"
            """);
        Files.writeString(configDir.resolve("pipeline.runtime.yaml"), """
            version: 1
            layout: modular
            validation: strict
            defaults:
              runtime: lambda
              module: per-step
            runtimes:
              lambda: {}
            modules:
              crawl-source-svc:
                runtime: lambda
            steps:
              ProcessCrawlSourceService:
                module: crawl-source-svc
            """);

        Path moduleDir = projectRoot.resolve("crawl-source-svc");
        Path generatedSourcesDir = moduleDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString().replace('\\', '/'),
                "-Apipeline.config=" + configDir.resolve("pipeline.modular-lambda.yaml").toString().replace('\\', '/'),
                "-Apipeline.module=crawl-source-svc",
                "-Apipeline.moduleDir=" + moduleDir.toString().replace('\\', '/'),
                "-Apipeline.transport=REST",
                "-Apipeline.platform=FUNCTION",
                "-Aprotobuf.descriptor.file="
                    + resourcePath("descriptor_set_search.dsc").toString().replace('\\', '/'))
            .compile(
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.crawl_source.service.ProcessCrawlSourceService",
                    """
                        package org.pipelineframework.search.crawl_source.service;

                        import io.smallrye.mutiny.Uni;
                        import org.pipelineframework.annotation.PipelineStep;
                        import org.pipelineframework.service.ReactiveService;

                        @PipelineStep
                        public class ProcessCrawlSourceService implements ReactiveService<
                                org.pipelineframework.search.common.domain.CrawlRequest,
                                org.pipelineframework.search.common.domain.RawDocument> {
                            @Override
                            public Uni<org.pipelineframework.search.common.domain.RawDocument> process(
                                    org.pipelineframework.search.common.domain.CrawlRequest input) {
                                return Uni.createFrom().item(new org.pipelineframework.search.common.domain.RawDocument());
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.common.domain.CrawlRequest",
                    """
                        package org.pipelineframework.search.common.domain;

                        public class CrawlRequest {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.common.domain.RawDocument",
                    """
                        package org.pipelineframework.search.common.domain;

                        public class RawDocument {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.crawl_source.dto.CrawlRequestDto",
                    """
                        package org.pipelineframework.search.crawl_source.dto;

                        public class CrawlRequestDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.crawl_source.dto.RawDocumentDto",
                    """
                        package org.pipelineframework.search.crawl_source.dto;

                        public class RawDocumentDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.crawl_source.mapper.CrawlRequestMapper",
                    """
                        package org.pipelineframework.search.crawl_source.mapper;

                        import org.pipelineframework.mapper.Mapper;
                        import org.pipelineframework.search.common.domain.CrawlRequest;
                        import org.pipelineframework.search.crawl_source.dto.CrawlRequestDto;

                        public class CrawlRequestMapper implements Mapper<CrawlRequest, CrawlRequestDto> {
                            @Override
                            public CrawlRequest fromExternal(CrawlRequestDto dto) {
                                return new CrawlRequest();
                            }

                            @Override
                            public CrawlRequestDto toExternal(CrawlRequest domain) {
                                return new CrawlRequestDto();
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "org.pipelineframework.search.crawl_source.mapper.RawDocumentMapper",
                    """
                        package org.pipelineframework.search.crawl_source.mapper;

                        import org.pipelineframework.mapper.Mapper;
                        import org.pipelineframework.search.common.domain.RawDocument;
                        import org.pipelineframework.search.crawl_source.dto.RawDocumentDto;

                        public class RawDocumentMapper implements Mapper<RawDocument, RawDocumentDto> {
                            @Override
                            public RawDocument fromExternal(RawDocumentDto dto) {
                                return new RawDocument();
                            }

                            @Override
                            public RawDocumentDto toExternal(RawDocument domain) {
                                return new RawDocumentDto();
                            }
                        }
                        """));

        assertThat(compilation).succeeded();

        Path restServerDir = generatedSourcesDir.resolve("rest-server");
        assertTrue(hasGeneratedClass(restServerDir, "ProcessCrawlSourceResource"));
        assertTrue(hasGeneratedClass(restServerDir, "ProcessCrawlSourceFunctionHandler"));
        assertTrue(compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", "roles.json").isPresent());
    }

    private Path resourcePath(String name) {
        URL resource = getClass().getClassLoader().getResource(name);
        if (resource == null) {
            throw new IllegalArgumentException("Missing test resource: " + name);
        }
        try {
            return Paths.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid test resource URI for: " + name, e);
        }
    }

    private boolean hasGeneratedClass(Path rootDir, String className) throws IOException {
        return Files.exists(findGeneratedClass(rootDir, className));
    }

    private Path findGeneratedClass(Path rootDir, String className) throws IOException {
        if (!Files.exists(rootDir)) {
            return rootDir.resolve(className + ".java");
        }
        try (var stream = Files.walk(rootDir)) {
            return stream.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> path.getFileName() != null
                    && path.getFileName().toString().equals(className + ".java"))
                .findFirst()
                .orElse(rootDir.resolve(className + ".java"));
        }
    }

    private String generatedJavaFiles(Path rootDir) throws IOException {
        if (!Files.exists(rootDir)) {
            return "[]";
        }
        try (var stream = Files.walk(rootDir)) {
            return stream.filter(path -> path.toString().endsWith(".java"))
                .map(rootDir::relativize)
                .map(Path::toString)
                .sorted()
                .toList()
                .toString();
        }
    }
}
