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
                cardinality: "ONE_TO_ONE"
                inputTypeName: "CrawlRequest"
                outputTypeName: "RawDocument"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Aprotobuf.descriptor.file=" + resourcePath("descriptor_set_search.dsc"))
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
                            stepType = StepOneToOne.class
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
                        """));

        assertThat(compilation).succeeded();

        Path grpcServerDir = generatedSourcesDir.resolve("pipeline-server");
        assertFalse(hasGeneratedClass(grpcServerDir, "ProcessCrawlSourceGrpcService"));
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
        if (!Files.exists(rootDir)) {
            return false;
        }
        try (var stream = Files.walk(rootDir)) {
            return stream.filter(path -> path.toString().endsWith(".java"))
                .anyMatch(path -> path.getFileName() != null
                    && path.getFileName().toString().equals(className + ".java"));
        }
    }
}
