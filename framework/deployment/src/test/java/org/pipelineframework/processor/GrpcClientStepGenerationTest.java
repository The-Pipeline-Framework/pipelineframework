package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import javax.tools.JavaFileObject;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that verifies gRPC client step generation using the CSV payments example.
 * This test ensures that the ProcessFolderService generates the correct client step
 * with gRPC message types instead of domain types.
 */
class GrpcClientStepGenerationTest {

    private static final String PROTOBUF_DESCRIPTOR_FILE = "protobuf.descriptor.file";

    @TempDir
    Path tempDir;

    @Test
    void testProcessFolderServiceClientStepGeneration() throws IOException {
        // Path to the descriptor file from test resources
        Path descriptorPath = Paths.get("src/test/resources/descriptor_set.dsc");

        assertTrue(descriptorPath.toFile().exists(),
            "Descriptor file should exist at " + descriptorPath.toAbsolutePath());

        // Create the source code for ProcessFolderService that should trigger client step generation
        JavaFileObject processFolderServiceSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.service.ProcessFolderService",
            """
            package org.pipelineframework.csv.service;

            import jakarta.enterprise.context.ApplicationScoped;
            import io.smallrye.mutiny.Multi;
            import org.pipelineframework.annotation.PipelineStep;
            import org.pipelineframework.csv.common.domain.CsvFolder;
            import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
            import org.pipelineframework.csv.common.mapper.CsvFolderMapper;
            import org.pipelineframework.csv.common.mapper.CsvPaymentsInputFileMapper;
            import org.pipelineframework.service.ReactiveStreamingService;

            @ApplicationScoped
            @PipelineStep(
                inputType = CsvFolder.class,
                outputType = CsvPaymentsInputFile.class,
                stepType = org.pipelineframework.step.StepOneToMany.class,
                backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
                inboundMapper = CsvFolderMapper.class,
                outboundMapper = CsvPaymentsInputFileMapper.class
            )
            public class ProcessFolderService implements ReactiveStreamingService<CsvFolder, CsvPaymentsInputFile> {

                public Multi<CsvPaymentsInputFile> process(CsvFolder csvFolder) {
                    return null; // Implementation not needed for this test
                }
            }
            """
        );

        // Create mock domain classes
        JavaFileObject csvFolderSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.domain.CsvFolder",
            """
            package org.pipelineframework.csv.common.domain;

            public class CsvFolder {
                private String path;

                public String getPath() {
                    return path;
                }

                public void setPath(String path) {
                    this.path = path;
                }
            }
            """
        );

        JavaFileObject csvPaymentsInputFileSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.domain.CsvPaymentsInputFile",
            """
            package org.pipelineframework.csv.common.domain;

            public class CsvPaymentsInputFile {
                private String id;
                private String filepath;
                private String csvFolderPath;

                public CsvPaymentsInputFile() {}

                public CsvPaymentsInputFile(java.io.File file) {
                    this.filepath = file.getAbsolutePath();
                }

                public String getId() {
                    return id;
                }

                public void setId(String id) {
                    this.id = id;
                }

                public String getFilepath() {
                    return filepath;
                }

                public void setFilepath(String filepath) {
                    this.filepath = filepath;
                }

                public String getCsvFolderPath() {
                    return csvFolderPath;
                }

                public void setCsvFolderPath(String csvFolderPath) {
                    this.csvFolderPath = csvFolderPath;
                }
            }
            """
        );

        // Create mock mapper classes
        JavaFileObject csvFolderMapperSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.mapper.CsvFolderMapper",
            """
            package org.pipelineframework.csv.common.mapper;

            import org.mapstruct.Mapper;
            import org.mapstruct.Mapping;
            import org.mapstruct.factory.Mappers;
            import org.pipelineframework.csv.common.domain.CsvFolder;
            import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;

            @Mapper
            public interface CsvFolderMapper extends org.pipelineframework.mapper.Mapper<InputCsvFileProcessingSvc.CsvFolder, InputCsvFileProcessingSvc.CsvFolder, CsvFolder> {
                CsvFolderMapper INSTANCE = Mappers.getMapper(CsvFolderMapper.class);

                @Override
                default InputCsvFileProcessingSvc.CsvFolder fromGrpc(InputCsvFileProcessingSvc.CsvFolder grpc) {
                    return grpc;
                }

                @Override
                default InputCsvFileProcessingSvc.CsvFolder toGrpc(InputCsvFileProcessingSvc.CsvFolder dto) {
                    return dto;
                }

                @Mapping(target = "path", source = "path")
                InputCsvFileProcessingSvc.CsvFolder toDto(CsvFolder domain);

                @Mapping(target = "path", source = "path")
                CsvFolder fromDto(InputCsvFileProcessingSvc.CsvFolder grpc);
            }
            """
        );

        JavaFileObject csvPaymentsInputFileMapperSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.mapper.CsvPaymentsInputFileMapper",
            """
            package org.pipelineframework.csv.common.mapper;

            import org.mapstruct.Mapper;
            import org.mapstruct.Mapping;
            import org.mapstruct.factory.Mappers;
            import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
            import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;

            @Mapper
            public interface CsvPaymentsInputFileMapper extends org.pipelineframework.mapper.Mapper<InputCsvFileProcessingSvc.CsvPaymentsInputFile, InputCsvFileProcessingSvc.CsvPaymentsInputFile, CsvPaymentsInputFile> {
                CsvPaymentsInputFileMapper INSTANCE = Mappers.getMapper(CsvPaymentsInputFileMapper.class);

                @Override
                default InputCsvFileProcessingSvc.CsvPaymentsInputFile fromGrpc(InputCsvFileProcessingSvc.CsvPaymentsInputFile grpc) {
                    return grpc;
                }

                @Override
                default InputCsvFileProcessingSvc.CsvPaymentsInputFile toGrpc(InputCsvFileProcessingSvc.CsvPaymentsInputFile dto) {
                    return dto;
                }

                @Mapping(target = "id", source = "id")
                @Mapping(target = "filepath", source = "filepath")
                @Mapping(target = "csvFolderPath", source = "csvFolderPath")
                InputCsvFileProcessingSvc.CsvPaymentsInputFile toDto(CsvPaymentsInputFile domain);

                @Mapping(target = "id", source = "id")
                @Mapping(target = "filepath", source = "filepath")
                @Mapping(target = "csvFolderPath", source = "csvFolderPath")
                CsvPaymentsInputFile fromDto(InputCsvFileProcessingSvc.CsvPaymentsInputFile grpc);
            }
            """
        );

        // Create mock gRPC classes (these would normally be generated from proto files)
        JavaFileObject inputCsvFileProcessingSvcSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc",
            """
            package org.pipelineframework.csv.grpc;

            // Mock gRPC message classes
            public class InputCsvFileProcessingSvc {
                public static class CsvFolder {
                    private String path;

                    public String getPath() {
                        return path;
                    }

                    public void setPath(String path) {
                        this.path = path;
                    }
                }

                public static class CsvPaymentsInputFile {
                    private String id;
                    private String filepath;
                    private String csvFolderPath;

                    public String getId() {
                        return id;
                    }

                    public void setId(String id) {
                        this.id = id;
                    }

                    public String getFilepath() {
                        return filepath;
                    }

                    public void setFilepath(String filepath) {
                        this.filepath = filepath;
                    }

                    public String getCsvFolderPath() {
                        return csvFolderPath;
                    }

                    public void setCsvFolderPath(String csvFolderPath) {
                        this.csvFolderPath = csvFolderPath;
                    }
                }

                public static class PaymentRecord {
                    private String id;
                    private String csvId;
                    private String recipient;
                    private String amount;
                    private String currency;
                    private String csvPaymentsInputFilePath;

                    // Getters and setters
                    public String getId() { return id; }
                    public void setId(String id) { this.id = id; }
                    public String getCsvId() { return csvId; }
                    public void setCsvId(String csvId) { this.csvId = csvId; }
                    public String getRecipient() { return recipient; }
                    public void setRecipient(String recipient) { this.recipient = recipient; }
                    public String getAmount() { return amount; }
                    public void setAmount(String amount) { this.amount = amount; }
                    public String getCurrency() { return currency; }
                    public void setCurrency(String currency) { this.currency = currency; }
                    public String getCsvPaymentsInputFilePath() { return csvPaymentsInputFilePath; }
                    public void setCsvPaymentsInputFilePath(String csvPaymentsInputFilePath) { this.csvPaymentsInputFilePath = csvPaymentsInputFilePath; }
                }
            }
            """
        );

        JavaFileObject mutinyProcessFolderServiceGrpcSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.grpc.MutinyProcessFolderServiceGrpc",
            """
            package org.pipelineframework.csv.grpc;

            import io.smallrye.mutiny.Multi;
            import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;

            // Mock gRPC stub class
            public class MutinyProcessFolderServiceGrpc {
                public static class MutinyProcessFolderServiceStub {
                    public Multi<InputCsvFileProcessingSvc.CsvPaymentsInputFile> remoteProcess(InputCsvFileProcessingSvc.CsvFolder request) {
                        return null; // Mock implementation
                    }
                }

                public static abstract class ProcessFolderServiceImplBase {
                    public io.smallrye.mutiny.Multi<InputCsvFileProcessingSvc.CsvPaymentsInputFile> remoteProcess(InputCsvFileProcessingSvc.CsvFolder request) {
                        return null; // Mock implementation
                    }
                }
            }
            """
        );

        // Create mock pipeline framework classes
        JavaFileObject pipelineStepAnnotation = JavaFileObjects.forSourceString(
            "org.pipelineframework.annotation.PipelineStep",
            """
            package org.pipelineframework.annotation;

            import java.lang.annotation.ElementType;
            import java.lang.annotation.Retention;
            import java.lang.annotation.RetentionPolicy;
            import java.lang.annotation.Target;

            @Target(ElementType.TYPE)
            @Retention(RetentionPolicy.SOURCE)
            public @interface PipelineStep {
                Class<?> inputType();
                Class<?> outputType();
                Class<?> stepType();
                Class<?> backendType();
                Class<?> inboundMapper() default Void.class;
                Class<?> outboundMapper() default Void.class;
                boolean runOnVirtualThreads() default false;
                Class<?> sideEffect() default Void.class;
            }
            """
        );

        JavaFileObject stepOneToManyInterface = JavaFileObjects.forSourceString(
            "org.pipelineframework.step.StepOneToMany",
            """
            package org.pipelineframework.step;

            import io.smallrye.mutiny.Multi;

            public interface StepOneToMany<T, S> {
                Multi<S> applyOneToMany(T input);
            }
            """
        );

        JavaFileObject reactiveStreamingServiceInterface = JavaFileObjects.forSourceString(
            "org.pipelineframework.service.ReactiveStreamingService",
            """
            package org.pipelineframework.service;

            import io.smallrye.mutiny.Multi;

            public interface ReactiveStreamingService<T, S> {
                Multi<S> process(T input);
            }
            """
        );

        JavaFileObject configurableStepClass = JavaFileObjects.forSourceString(
            "org.pipelineframework.step.ConfigurableStep",
            """
            package org.pipelineframework.step;

            public class ConfigurableStep {
                // Base class for configurable steps
            }
            """
        );

        JavaFileObject grpcReactiveServiceAdapterClass = JavaFileObjects.forSourceString(
            "org.pipelineframework.grpc.GrpcReactiveServiceAdapter",
            """
            package org.pipelineframework.grpc;

            public class GrpcReactiveServiceAdapter {
                // Base class for gRPC reactive service adapters
            }
            """
        );

        // Create the processor options with the descriptor file path
        Map<String, String> options = Collections.singletonMap(
            PROTOBUF_DESCRIPTOR_FILE,
            descriptorPath.toString()
        );

        // Compile with the PipelineStepProcessor and verify the generated client step
        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Xlint:unchecked",
                "-A" + PROTOBUF_DESCRIPTOR_FILE + "=" + descriptorPath.toString(),
                "-Apipeline.generatedSourcesDir=" + tempDir.toString()
            )
            .compile(
                processFolderServiceSource,
                csvFolderSource,
                csvPaymentsInputFileSource,
                csvFolderMapperSource,
                csvPaymentsInputFileMapperSource,
                inputCsvFileProcessingSvcSource,
                mutinyProcessFolderServiceGrpcSource,
                pipelineStepAnnotation,
                stepOneToManyInterface,
                reactiveStreamingServiceInterface,
                configurableStepClass,
                grpcReactiveServiceAdapterClass
            );

        // Verify compilation was successful
        assertEquals(Compilation.Status.SUCCESS, compilation.status(), 
            "Compilation should succeed with no errors: " + compilation.diagnostics());

        Path generatedSource = tempDir.resolve(
            "orchestrator-client/org/pipelineframework/csv/service/pipeline/ProcessFolderGrpcClientStep.java");
        assertTrue(Files.exists(generatedSource), "ProcessFolderGrpcClientStep should be generated");

        String sourceCode = Files.readString(generatedSource);

        // Check that the generated client step uses gRPC types
        assertTrue(sourceCode.contains("InputCsvFileProcessingSvc.CsvFolder"),
            "Client step should use gRPC input type: " + sourceCode);
        assertTrue(sourceCode.contains("InputCsvFileProcessingSvc.CsvPaymentsInputFile"),
            "Client step should use gRPC output type: " + sourceCode);
        assertTrue(sourceCode.contains("MutinyProcessFolderServiceGrpc.MutinyProcessFolderServiceStub"),
            "Client step should use correct gRPC stub type: " + sourceCode);
        assertTrue(sourceCode.contains("StepOneToMany<InputCsvFileProcessingSvc.CsvFolder, InputCsvFileProcessingSvc.CsvPaymentsInputFile>"),
            "Client step should implement StepOneToMany with gRPC types: " + sourceCode);
    }

    @Test
    void testProcessFolderServiceGrpcServiceAdapterGeneration() throws IOException {
        // Path to the descriptor file from test resources
        Path descriptorPath = Paths.get("src/test/resources/descriptor_set.dsc");

        assertTrue(descriptorPath.toFile().exists(),
            "Descriptor file should exist at " + descriptorPath.toAbsolutePath());

        // Create the source code for ProcessFolderService that should trigger gRPC service adapter generation
        JavaFileObject processFolderServiceSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.service.ProcessFolderService",
            """
            package org.pipelineframework.csv.service;

            import jakarta.enterprise.context.ApplicationScoped;
            import io.smallrye.mutiny.Multi;
            import org.pipelineframework.annotation.PipelineStep;
            import org.pipelineframework.csv.common.domain.CsvFolder;
            import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
            import org.pipelineframework.csv.common.mapper.CsvFolderMapper;
            import org.pipelineframework.csv.common.mapper.CsvPaymentsInputFileMapper;
            import org.pipelineframework.service.ReactiveStreamingService;

            @ApplicationScoped
            @PipelineStep(
                inputType = CsvFolder.class,
                outputType = CsvPaymentsInputFile.class,
                stepType = org.pipelineframework.step.StepOneToMany.class,
                backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
                inboundMapper = CsvFolderMapper.class,
                outboundMapper = CsvPaymentsInputFileMapper.class
            )
            public class ProcessFolderService implements ReactiveStreamingService<CsvFolder, CsvPaymentsInputFile> {

                public Multi<CsvPaymentsInputFile> process(CsvFolder csvFolder) {
                    return null; // Implementation not needed for this test
                }
            }
            """
        );

        // Create mock domain classes
        JavaFileObject csvFolderSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.domain.CsvFolder",
            """
            package org.pipelineframework.csv.common.domain;

            public class CsvFolder {
                private String path;

                public String getPath() {
                    return path;
                }

                public void setPath(String path) {
                    this.path = path;
                }
            }
            """
        );

        JavaFileObject csvPaymentsInputFileSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.domain.CsvPaymentsInputFile",
            """
            package org.pipelineframework.csv.common.domain;

            public class CsvPaymentsInputFile {
                private String id;
                private String filepath;
                private String csvFolderPath;

                public CsvPaymentsInputFile() {}

                public CsvPaymentsInputFile(java.io.File file) {
                    this.filepath = file.getAbsolutePath();
                }

                public String getId() {
                    return id;
                }

                public void setId(String id) {
                    this.id = id;
                }

                public String getFilepath() {
                    return filepath;
                }

                public void setFilepath(String filepath) {
                    this.filepath = filepath;
                }

                public String getCsvFolderPath() {
                    return csvFolderPath;
                }

                public void setCsvFolderPath(String csvFolderPath) {
                    this.csvFolderPath = csvFolderPath;
                }
            }
            """
        );

        // Create mock mapper classes
        JavaFileObject csvFolderMapperSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.mapper.CsvFolderMapper",
            """
            package org.pipelineframework.csv.common.mapper;

            import org.mapstruct.Mapper;
            import org.mapstruct.Mapping;
            import org.mapstruct.factory.Mappers;
            import org.pipelineframework.csv.common.domain.CsvFolder;
            import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;

            @Mapper
            public interface CsvFolderMapper extends org.pipelineframework.mapper.Mapper<InputCsvFileProcessingSvc.CsvFolder, InputCsvFileProcessingSvc.CsvFolder, CsvFolder> {
                CsvFolderMapper INSTANCE = Mappers.getMapper(CsvFolderMapper.class);

                @Override
                default InputCsvFileProcessingSvc.CsvFolder fromGrpc(InputCsvFileProcessingSvc.CsvFolder grpc) {
                    return grpc;
                }

                @Override
                default InputCsvFileProcessingSvc.CsvFolder toGrpc(InputCsvFileProcessingSvc.CsvFolder dto) {
                    return dto;
                }

                @Mapping(target = "path", source = "path")
                InputCsvFileProcessingSvc.CsvFolder toDto(CsvFolder domain);

                @Mapping(target = "path", source = "path")
                CsvFolder fromDto(InputCsvFileProcessingSvc.CsvFolder grpc);
            }
            """
        );

        JavaFileObject csvPaymentsInputFileMapperSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.common.mapper.CsvPaymentsInputFileMapper",
            """
            package org.pipelineframework.csv.common.mapper;

            import org.mapstruct.Mapper;
            import org.mapstruct.Mapping;
            import org.mapstruct.factory.Mappers;
            import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
            import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;

            @Mapper
            public interface CsvPaymentsInputFileMapper extends org.pipelineframework.mapper.Mapper<InputCsvFileProcessingSvc.CsvPaymentsInputFile, InputCsvFileProcessingSvc.CsvPaymentsInputFile, CsvPaymentsInputFile> {
                CsvPaymentsInputFileMapper INSTANCE = Mappers.getMapper(CsvPaymentsInputFileMapper.class);

                @Override
                default InputCsvFileProcessingSvc.CsvPaymentsInputFile fromGrpc(InputCsvFileProcessingSvc.CsvPaymentsInputFile grpc) {
                    return grpc;
                }

                @Override
                default InputCsvFileProcessingSvc.CsvPaymentsInputFile toGrpc(InputCsvFileProcessingSvc.CsvPaymentsInputFile dto) {
                    return dto;
                }

                @Mapping(target = "id", source = "id")
                @Mapping(target = "filepath", source = "filepath")
                @Mapping(target = "csvFolderPath", source = "csvFolderPath")
                InputCsvFileProcessingSvc.CsvPaymentsInputFile toDto(CsvPaymentsInputFile domain);

                @Mapping(target = "id", source = "id")
                @Mapping(target = "filepath", source = "filepath")
                @Mapping(target = "csvFolderPath", source = "csvFolderPath")
                CsvPaymentsInputFile fromDto(InputCsvFileProcessingSvc.CsvPaymentsInputFile grpc);
            }
            """
        );

        // Create mock gRPC classes (these would normally be generated from proto files)
        JavaFileObject inputCsvFileProcessingSvcSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc",
            """
            package org.pipelineframework.csv.grpc;

            // Mock gRPC message classes
            public class InputCsvFileProcessingSvc {
                public static class CsvFolder {
                    private String path;

                    public String getPath() {
                        return path;
                    }

                    public void setPath(String path) {
                        this.path = path;
                    }
                }

                public static class CsvPaymentsInputFile {
                    private String id;
                    private String filepath;
                    private String csvFolderPath;

                    public String getId() {
                        return id;
                    }

                    public void setId(String id) {
                        this.id = id;
                    }

                    public String getFilepath() {
                        return filepath;
                    }

                    public void setFilepath(String filepath) {
                        this.filepath = filepath;
                    }

                    public String getCsvFolderPath() {
                        return csvFolderPath;
                    }

                    public void setCsvFolderPath(String csvFolderPath) {
                        this.csvFolderPath = csvFolderPath;
                    }
                }

                public static class PaymentRecord {
                    private String id;
                    private String csvId;
                    private String recipient;
                    private String amount;
                    private String currency;
                    private String csvPaymentsInputFilePath;

                    // Getters and setters
                    public String getId() { return id; }
                    public void setId(String id) { this.id = id; }
                    public String getCsvId() { return csvId; }
                    public void setCsvId(String csvId) { this.csvId = csvId; }
                    public String getRecipient() { return recipient; }
                    public void setRecipient(String recipient) { this.recipient = recipient; }
                    public String getAmount() { return amount; }
                    public void setAmount(String amount) { this.amount = amount; }
                    public String getCurrency() { return currency; }
                    public void setCurrency(String currency) { this.currency = currency; }
                    public String getCsvPaymentsInputFilePath() { return csvPaymentsInputFilePath; }
                    public void setCsvPaymentsInputFilePath(String csvPaymentsInputFilePath) { this.csvPaymentsInputFilePath = csvPaymentsInputFilePath; }
                }
            }
            """
        );

        JavaFileObject mutinyProcessFolderServiceGrpcSource = JavaFileObjects.forSourceString(
            "org.pipelineframework.csv.grpc.MutinyProcessFolderServiceGrpc",
            """
            package org.pipelineframework.csv.grpc;

            import io.smallrye.mutiny.Multi;
            import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;

            // Mock gRPC stub and service classes
            public class MutinyProcessFolderServiceGrpc {
                public static class MutinyProcessFolderServiceStub {
                    public Multi<InputCsvFileProcessingSvc.CsvPaymentsInputFile> remoteProcess(InputCsvFileProcessingSvc.CsvFolder request) {
                        return null; // Mock implementation
                    }
                }

                public static abstract class ProcessFolderServiceImplBase {
                    public io.smallrye.mutiny.Multi<InputCsvFileProcessingSvc.CsvPaymentsInputFile> remoteProcess(InputCsvFileProcessingSvc.CsvFolder request) {
                        return null; // Mock implementation
                    }
                }
            }
            """
        );

        // Create mock pipeline framework classes
        JavaFileObject pipelineStepAnnotation = JavaFileObjects.forSourceString(
            "org.pipelineframework.annotation.PipelineStep",
            """
            package org.pipelineframework.annotation;

            import java.lang.annotation.ElementType;
            import java.lang.annotation.Retention;
            import java.lang.annotation.RetentionPolicy;
            import java.lang.annotation.Target;

            @Target(ElementType.TYPE)
            @Retention(RetentionPolicy.SOURCE)
            public @interface PipelineStep {
                Class<?> inputType();
                Class<?> outputType();
                Class<?> stepType();
                Class<?> backendType();
                Class<?> inboundMapper() default Void.class;
                Class<?> outboundMapper() default Void.class;
                boolean runOnVirtualThreads() default false;
                Class<?> sideEffect() default Void.class;
            }
            """
        );

        JavaFileObject stepOneToManyInterface = JavaFileObjects.forSourceString(
            "org.pipelineframework.step.StepOneToMany",
            """
            package org.pipelineframework.step;

            import io.smallrye.mutiny.Multi;

            public interface StepOneToMany<T, S> {
                Multi<S> applyOneToMany(T input);
            }
            """
        );

        JavaFileObject reactiveStreamingServiceInterface = JavaFileObjects.forSourceString(
            "org.pipelineframework.service.ReactiveStreamingService",
            """
            package org.pipelineframework.service;

            import io.smallrye.mutiny.Multi;

            public interface ReactiveStreamingService<T, S> {
                Multi<S> process(T input);
            }
            """
        );

        JavaFileObject grpcReactiveServiceAdapterClass = JavaFileObjects.forSourceString(
            "org.pipelineframework.grpc.GrpcReactiveServiceAdapter",
            """
            package org.pipelineframework.grpc;

            public class GrpcReactiveServiceAdapter {
                // Base class for gRPC reactive service adapters
            }
            """
        );

        JavaFileObject grpcServiceStreamingAdapterClass = JavaFileObjects.forSourceString(
            "org.pipelineframework.grpc.GrpcServiceStreamingAdapter",
            """
            package org.pipelineframework.grpc;

            import io.smallrye.mutiny.Multi;

            public abstract class GrpcServiceStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> {
                protected abstract Object getService();
                protected abstract DomainIn fromGrpc(GrpcIn grpcIn);
                protected abstract GrpcOut toGrpc(DomainOut output);

                public Multi<GrpcOut> remoteProcess(GrpcIn request) {
                    return null; // Mock implementation
                }
            }
            """
        );

        // Create the processor options with the descriptor file path
        Map<String, String> options = Collections.singletonMap(
            PROTOBUF_DESCRIPTOR_FILE,
            descriptorPath.toString()
        );

        // Compile with the PipelineStepProcessor and verify the generated gRPC service adapter
        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Xlint:unchecked",
                "-A" + PROTOBUF_DESCRIPTOR_FILE + "=" + descriptorPath.toString(),
                "-Apipeline.generatedSourcesDir=" + tempDir.toString()
            )
            .compile(
                processFolderServiceSource,
                csvFolderSource,
                csvPaymentsInputFileSource,
                csvFolderMapperSource,
                csvPaymentsInputFileMapperSource,
                inputCsvFileProcessingSvcSource,
                mutinyProcessFolderServiceGrpcSource,
                pipelineStepAnnotation,
                stepOneToManyInterface,
                reactiveStreamingServiceInterface,
                grpcReactiveServiceAdapterClass,
                grpcServiceStreamingAdapterClass
            );

        // Verify compilation was successful
        assertEquals(Compilation.Status.SUCCESS, compilation.status(), 
            "Compilation should succeed with no errors: " + compilation.diagnostics());

        Path generatedSource = tempDir.resolve(
            "pipeline-server/org/pipelineframework/csv/service/pipeline/ProcessFolderServiceGrpcService.java");
        assertTrue(Files.exists(generatedSource), "ProcessFolderServiceGrpcService should be generated");

        String sourceCode = Files.readString(generatedSource);

        // Check that the generated gRPC service adapter uses gRPC types in the adapter
        assertTrue(sourceCode.contains("GrpcServiceStreamingAdapter<InputCsvFileProcessingSvc.CsvFolder, InputCsvFileProcessingSvc.CsvPaymentsInputFile, CsvFolder, CsvPaymentsInputFile>"),
            "gRPC service adapter should use correct type parameters: " + sourceCode);
        assertTrue(sourceCode.contains("InputCsvFileProcessingSvc.CsvFolder grpcIn"),
            "gRPC service adapter should have correct parameter type: " + sourceCode);
        assertTrue(sourceCode.contains("InputCsvFileProcessingSvc.CsvPaymentsInputFile toGrpc"),
            "gRPC service adapter should have correct return type: " + sourceCode);
    }
}
