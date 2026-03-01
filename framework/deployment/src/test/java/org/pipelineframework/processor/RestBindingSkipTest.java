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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RestBindingSkipTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesRestResourcesWhenRestTransportConfigured() throws IOException {
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
              - name: "Process Foo"
                service: "com.example.ProcessFooService"
                input: "com.example.domain.FooInput"
                output: "com.example.domain.FooOutput"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor(), new org.mapstruct.ap.MappingProcessor())
            .withOptions("-Apipeline.generatedSourcesDir=" + generatedSourcesDir)
            .compile(
                JavaFileObjects.forSourceString(
                    "com.example.ProcessFooService",
                    """
                        package com.example;

                        import io.smallrye.mutiny.Uni;
                        import org.pipelineframework.annotation.PipelineStep;
                        import org.pipelineframework.service.ReactiveService;
                        import org.pipelineframework.step.StepOneToOne;

                        @PipelineStep(
                            inputType = com.example.domain.FooInput.class,
                            outputType = com.example.domain.FooOutput.class,
                            stepType = StepOneToOne.class
                        )
                        public class ProcessFooService implements ReactiveService<com.example.domain.FooInput, com.example.domain.FooOutput> {
                            @Override
                            public Uni<com.example.domain.FooOutput> process(com.example.domain.FooInput input) {
                                return Uni.createFrom().item(input == null ? null : new com.example.domain.FooOutput());
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.domain.FooInput",
                    """
                        package com.example.domain;

                        public class FooInput {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.domain.FooOutput",
                    """
                        package com.example.domain;

                        public class FooOutput {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.dto.FooInputDto",
                    """
                        package com.example.dto;

                        public class FooInputDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.dto.FooOutputDto",
                    """
                        package com.example.dto;

                        public class FooOutputDto {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.grpc.FooInputGrpcMessage",
                    """
                        package com.example.grpc;

                        public class FooInputGrpcMessage {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.grpc.FooOutputGrpcMessage",
                    """
                        package com.example.grpc;

                        public class FooOutputGrpcMessage {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.mapper.FooInputMapper",
                    """
                        package com.example.mapper;

                        import com.example.domain.FooInput;
                        import com.example.dto.FooInputDto;
                        import com.example.grpc.FooInputGrpcMessage;
                        import org.pipelineframework.mapper.Mapper;

                        @org.mapstruct.Mapper
                        public interface FooInputMapper extends Mapper<FooInput, FooInputDto> {
                            FooInput fromExternal(FooInputDto dto);
                            FooInputDto toExternal(FooInput domain);
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.mapper.FooOutputMapper",
                    """
                        package com.example.mapper;

                        import com.example.domain.FooOutput;
                        import com.example.dto.FooOutputDto;
                        import com.example.grpc.FooOutputGrpcMessage;
                        import org.pipelineframework.mapper.Mapper;

                        @org.mapstruct.Mapper
                        public interface FooOutputMapper extends Mapper<FooOutput, FooOutputDto> {
                            FooOutput fromExternal(FooOutputDto dto);
                            FooOutputDto toExternal(FooOutput domain);
                        }
                        """));

        assertThat(compilation).succeeded();

        Path restServerDir = generatedSourcesDir.resolve("rest-server");
        Path grpcServerDir = generatedSourcesDir.resolve("pipeline-server");
        assertTrue(hasGeneratedClass(restServerDir, "ProcessFooResource"));
        assertFalse(hasGeneratedClass(grpcServerDir, "ProcessFooGrpcService"));
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
}
