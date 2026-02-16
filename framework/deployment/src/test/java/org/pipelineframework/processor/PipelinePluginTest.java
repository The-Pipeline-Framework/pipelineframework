package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;

/**
 * Integration test for PipelinePlugin annotation processing.
 * This test verifies that when we process a marker/host class annotated with
 * PipelinePlugin(name), it produces server adapters/resources for ALL the
 * expanded/synthetic steps (for the aspect).
 */
class PipelinePluginTest {

    @TempDir
    Path tempDir;

    @Test
    void testPipelinePluginGeneratesServerAdapters() throws IOException {
        // Create the proper directory structure for the module
        // The locator looks for parent POM with packaging=pom, so create that structure
        Path projectRoot = tempDir;
        // Create a pom.xml with packaging=pom to serve as the project root
        Path rootPomPath = projectRoot.resolve("pom.xml");
        Files.writeString(rootPomPath, """
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

        // Create module directory under project root
        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target").resolve("generated-sources").resolve("pipeline");
        Files.createDirectories(generatedSourcesDir);

        // Copy the descriptor file to the temp directory since we need it for the test
        Path descriptorPath = Paths.get(System.getProperty("user.dir")).resolve("src/test/resources/descriptor_set.dsc");
        if (Files.exists(descriptorPath)) {
            Path targetDescriptorPath = moduleDir.resolve("descriptor_set.dsc");
            Files.createDirectories(targetDescriptorPath.getParent());
            Files.copy(descriptorPath, targetDescriptorPath);
        }

        // Create the pipeline.yaml file in the project root (where locator will find it)
        Path pipelineConfig = projectRoot.resolve("pipeline.yaml");
        Files.writeString(pipelineConfig, """
            appName: "Test Pipeline"
            basePackage: "test.plugin"
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
                config:
                  pluginImplementationClass: "test.plugin.PersistenceService"
            """);

        // Define a plugin host class
        String pluginCode = """
            package test.plugin;

            import org.pipelineframework.annotation.PipelinePlugin;

            @PipelinePlugin("persistence")
            public class PersistencePluginHost {
            }
            """;

        // Compile with the PipelineStepProcessor
        Compilation compilation = Compiler.javac()
            .withProcessors(new org.pipelineframework.processor.PipelineStepProcessor(), new org.mapstruct.ap.MappingProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString(),
                "-Aprotobuf.descriptor.file=" + moduleDir.resolve("descriptor_set.dsc").toString())
            .compile(
                JavaFileObjects.forSourceString("test.plugin.PersistencePluginHost", pluginCode),
                // Service class with @PipelineStep
                JavaFileObjects.forSourceString("test.plugin.ProcessTestService", """
                    package test.plugin;

                    import org.pipelineframework.annotation.PipelineStep;
                    import org.pipelineframework.step.StepOneToOne;
                    import test.plugin.common.domain.TestData;

                    @PipelineStep(
                        inputType = TestData.class,
                        outputType = TestData.class,
                        stepType = StepOneToOne.class
                    )
                    public class ProcessTestService {
                    }
                    """),
                // Domain type stub
                JavaFileObjects.forSourceString("test.plugin.common.domain.TestData", """
                    package test.plugin.common.domain;
                    public class TestData { }
                    """),
                // Mapper stub for TestData domain type
                JavaFileObjects.forSourceString("test.plugin.common.mapper.TestDataMapper", """
                    package test.plugin.common.mapper;

                    import org.pipelineframework.mapper.Mapper;
                    import test.plugin.common.domain.TestData;
                    import test.plugin.common.domain.TestDataGrpcMessage;

                    @org.mapstruct.Mapper
                    public interface TestDataMapper extends Mapper<TestDataGrpcMessage, TestData, TestData> {
                        TestData fromGrpc(TestDataGrpcMessage grpc);
                        TestDataGrpcMessage toGrpc(TestData dto);
                        TestData fromDto(TestData dto);
                        TestData toDto(TestData domain);
                    }
                    """),
                // gRPC message stub
                JavaFileObjects.forSourceString("test.plugin.common.domain.TestDataGrpcMessage", """
                    package test.plugin.common.domain;

                    public class TestDataGrpcMessage {
                    }
                    """)
            );

        // Verify compilation succeeded
        assertThat(compilation).succeeded();

        // Verify that REST server adapters/resources are generated for the regular steps when processing plugin host
        // Look for generated files in the expected locations
        Path restServerDir = generatedSourcesDir.resolve("rest-server");

        // Check if REST server files were generated for the regular steps
        boolean hasRestFiles = Files.exists(restServerDir) &&
            Files.walk(restServerDir)
                .filter(Files::isRegularFile)
                .anyMatch(path -> path.toString().endsWith(".java"));

        // If REST server files exist, verify they contain the expected content for the ProcessTestService REST resource
        if (hasRestFiles) {
            boolean hasCorrectContent = Files.walk(restServerDir)
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".java"))
                .anyMatch(path -> {
                    try {
                        String content = Files.readString(path);
                        // Check for expected content indicating it's a REST resource for the persistence aspect
                        return content.contains("class PersistenceTestDataSideEffectResource") &&
                               (content.contains("@Path") || content.contains("@GET") || content.contains("@POST"));
                    } catch (IOException e) {
                        return false;
                    }
                });

            if (!hasCorrectContent) {
                // List what files were actually generated for debugging
                try {
                    java.util.List<Path> files = Files.walk(restServerDir)
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".java"))
                        .toList();
                    System.out.println("Generated files: " + files);
                    for (Path file : files) {
                        System.out.println("Content of " + file.getFileName() + ":");
                        System.out.println(Files.readString(file));
                    }
                } catch (IOException e) {
                    // Ignore
                }
                throw new AssertionError("REST server files were generated but don't contain expected REST resource content for the persistence aspect");
            }
        } else {
            throw new AssertionError("No REST server files were generated for regular steps in plugin host");
        }
    }

    @Test
    void testPipelinePluginWithMultipleAspects() throws IOException {
        // Create the proper directory structure for the module
        // The locator looks for parent POM with packaging=pom, so create that structure
        Path projectRoot = tempDir;
        // Create a pom.xml with packaging=pom to serve as the project root
        Path rootPomPath = projectRoot.resolve("pom.xml");
        Files.writeString(rootPomPath, """
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

        // Create module directory under project root
        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target").resolve("generated-sources").resolve("pipeline");
        Files.createDirectories(generatedSourcesDir);

        // Copy the descriptor file to the temp directory since we need it for the test
        Path descriptorPath = Paths.get(System.getProperty("user.dir")).resolve("src/test/resources/descriptor_set.dsc");
        if (Files.exists(descriptorPath)) {
            Path targetDescriptorPath = moduleDir.resolve("descriptor_set.dsc");
            Files.createDirectories(targetDescriptorPath.getParent());
            Files.copy(descriptorPath, targetDescriptorPath);
        }

        // Create the pipeline.yaml file in the project root (where locator will find it)
        Path pipelineConfig = projectRoot.resolve("pipeline.yaml");
        Files.writeString(pipelineConfig, """
            appName: "Test Pipeline"
            basePackage: "test.plugin"
            transport: "REST"
            steps:
              - name: "Process Test"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "TestData"
                outputTypeName: "TestData"
            aspects:
              cache:
                enabled: true
                scope: "GLOBAL"
                position: "AFTER_STEP"
                config:
                  pluginImplementationClass: "test.plugin.CacheService"
            """);

        // Define a plugin host class for cache aspect
        String pluginCode = """
            package test.plugin;

            import org.pipelineframework.annotation.PipelinePlugin;

            @PipelinePlugin("cache")
            public class CachePluginHost {
            }
            """;

        // Compile with the PipelineStepProcessor
        Compilation compilation = Compiler.javac()
            .withProcessors(new org.pipelineframework.processor.PipelineStepProcessor(), new org.mapstruct.ap.MappingProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString(),
                "-Aprotobuf.descriptor.file=" + moduleDir.resolve("descriptor_set.dsc").toString())
            .compile(
                JavaFileObjects.forSourceString("test.plugin.CachePluginHost", pluginCode),
                // Service class with @PipelineStep
                JavaFileObjects.forSourceString("test.plugin.ProcessTestService", """
                    package test.plugin;

                    import org.pipelineframework.annotation.PipelineStep;
                    import org.pipelineframework.step.StepOneToOne;
                    import test.plugin.common.domain.TestData;

                    @PipelineStep(
                        inputType = TestData.class,
                        outputType = TestData.class,
                        stepType = StepOneToOne.class
                    )
                    public class ProcessTestService {
                    }
                    """),
                // Domain type stub
                JavaFileObjects.forSourceString("test.plugin.common.domain.TestData", """
                    package test.plugin.common.domain;
                    public class TestData { }
                    """),
                // Mapper stub for TestData domain type
                JavaFileObjects.forSourceString("test.plugin.common.mapper.TestDataMapper", """
                    package test.plugin.common.mapper;

                    import org.pipelineframework.mapper.Mapper;
                    import test.plugin.common.domain.TestData;
                    import test.plugin.common.domain.TestDataGrpcMessage;

                    @org.mapstruct.Mapper
                    public interface TestDataMapper extends Mapper<TestDataGrpcMessage, TestData, TestData> {
                        TestData fromGrpc(TestDataGrpcMessage grpc);
                        TestDataGrpcMessage toGrpc(TestData dto);
                        TestData fromDto(TestData dto);
                        TestData toDto(TestData domain);
                    }
                    """),
                // gRPC message stub
                JavaFileObjects.forSourceString("test.plugin.common.domain.TestDataGrpcMessage", """
                    package test.plugin.common.domain;

                    public class TestDataGrpcMessage {
                    }
                    """)
            );

        // Verify compilation succeeded
        assertThat(compilation).succeeded();

        // Verify that REST server adapters/resources are generated for the regular steps when processing plugin host
        // Look for generated files in the expected locations
        Path restServerDir = generatedSourcesDir.resolve("rest-server");

        // Check if REST server files were generated for the regular steps
        boolean hasRestFiles = Files.exists(restServerDir) &&
            Files.walk(restServerDir)
                .filter(Files::isRegularFile)
                .anyMatch(path -> path.toString().endsWith(".java"));

        // If REST server files exist, verify they contain the expected content for the ProcessTestService REST resource
        if (hasRestFiles) {
            boolean hasCorrectContent = Files.walk(restServerDir)
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".java"))
                .anyMatch(path -> {
                    try {
                        String content = Files.readString(path);
                        // Check for expected content indicating it's a REST resource for the cache aspect
                        return content.contains("class CacheTestDataSideEffectResource") &&
                               (content.contains("@Path") || content.contains("@GET") || content.contains("@POST"));
                    } catch (IOException e) {
                        return false;
                    }
                });

            if (!hasCorrectContent) {
                // List what files were actually generated for debugging
                try {
                    java.util.List<Path> files = Files.walk(restServerDir)
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".java"))
                        .toList();
                    System.out.println("Generated files: " + files);
                    for (Path file : files) {
                        System.out.println("Content of " + file.getFileName() + ":");
                        System.out.println(Files.readString(file));
                    }
                } catch (IOException e) {
                    // Ignore
                }
                throw new AssertionError("REST server files were generated but don't contain expected REST resource content for the cache aspect");
            }
        } else {
            throw new AssertionError("No REST server files were generated for regular steps in plugin host");
        }
    }
}
