package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;

class PluginProducerGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void allowsRestGenerationWithoutApTimeMapperResolution() throws IOException {
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
            basePackage: "com.example"
            transport: "REST"
            steps:
              - name: "Process Test"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "TestData"
                outputTypeName: "TestData"
            aspects:
              persistence:
                enabled: true
                scope: "GLOBAL"
                position: "AFTER_STEP"
                order: 0
                config:
                  pluginImplementationClass: "com.example.PersistenceService"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor(), new org.mapstruct.ap.MappingProcessor())
            .withOptions("-Apipeline.generatedSourcesDir=" + generatedSourcesDir)
            .compile(
                JavaFileObjects.forSourceString(
                    "com.example.PersistencePluginHost",
                    """
                        package com.example;

                        import org.pipelineframework.annotation.PipelinePlugin;

                        @PipelinePlugin("persistence")
                        public class PersistencePluginHost {
                        }
                        """),
                // Service class with @PipelineStep
                JavaFileObjects.forSourceString("com.example.ProcessTestService", """
                    package com.example;

                    import org.pipelineframework.annotation.PipelineStep;
                    import org.pipelineframework.step.StepOneToOne;
                    import com.example.common.domain.TestData;

                    @PipelineStep(
                        inputType = TestData.class,
                        outputType = TestData.class,
                        stepType = StepOneToOne.class
                    )
                    public class ProcessTestService {
                    }
                    """),
                // Domain type stub
                JavaFileObjects.forSourceString("com.example.common.domain.TestData", """
                    package com.example.common.domain;
                    public class TestData { }
                    """),
                // Mapper stub for TestData domain type
                JavaFileObjects.forSourceString("com.example.common.mapper.TestDataMapper", """
                    package com.example.common.mapper;

                    import org.pipelineframework.mapper.Mapper;
                    import com.example.common.domain.TestData;
                    import com.example.common.domain.TestDataGrpcMessage;

                    @org.mapstruct.Mapper
                    public interface TestDataMapper extends Mapper<TestDataGrpcMessage, TestData, TestData> {
                        TestData fromGrpc(TestDataGrpcMessage grpc);
                        TestDataGrpcMessage toGrpc(TestData dto);
                        TestData fromDto(TestData dto);
                        TestData toDto(TestData domain);
                    }
                    """),
                // gRPC message stub
                JavaFileObjects.forSourceString("com.example.common.domain.TestDataGrpcMessage", """
                    package com.example.common.domain;

                    public class TestDataGrpcMessage {
                    }
                    """)
            );

        assertThat(compilation).succeeded();
    }
}
