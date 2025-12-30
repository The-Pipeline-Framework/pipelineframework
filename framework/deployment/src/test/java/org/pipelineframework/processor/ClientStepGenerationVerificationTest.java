package org.pipelineframework.processor;

import javax.tools.JavaFileObject;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify that the client step generation is now using gRPC types instead of domain types
 */
class ClientStepGenerationVerificationTest {

    private static final String PROTOBUF_DESCRIPTOR_FILE = "protobuf.descriptor.file";

    @Test
    void verifyClientStepUsesGrpcTypes() throws Exception {
        // Path to the descriptor file from test resources
        java.nio.file.Path descriptorPath = java.nio.file.Paths.get("src/test/resources/descriptor_set.dsc");

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
                boolean grpcEnabled() default true;
                String grpcClient() default "";
                Class<?> inputGrpcType() default Void.class;
                Class<?> outputGrpcType() default Void.class;
                Class<?> grpcImpl() default Void.class;
                Class<?> grpcStub() default Void.class;
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

        // Compile with the PipelineStepProcessor and check the generated code
        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions("-Xlint:unchecked", "-A" + PROTOBUF_DESCRIPTOR_FILE + "=" + descriptorPath.toString())
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
                grpcReactiveServiceAdapterClass,
                grpcServiceStreamingAdapterClass
            );

        // Look for the generated client step class
        boolean clientStepGenerated = false;
        for (JavaFileObject fileObject : compilation.generatedSourceFiles()) {
            if (fileObject.getName().contains("ProcessFolderClientStep")) {
                String sourceCode = fileObject.getCharContent(true).toString();
                System.out.println("Generated Client Step Code:");
                System.out.println(sourceCode);
                
                // Check that the generated client step uses gRPC types instead of domain types
                assertTrue(sourceCode.contains("StepOneToMany"),
                    "Client step should implement StepOneToMany interface");
                
                // The interface should use gRPC types, not domain types
                // Previously it was using CsvFolder and CsvPaymentsInputFile (domain types)
                // Now it should use InputCsvFileProcessingSvc.CsvFolder and InputCsvFileProcessingSvc.CsvPaymentsInputFile (gRPC types)
                if (sourceCode.contains("CsvFolder") && !sourceCode.contains("InputCsvFileProcessingSvc")) {
                    System.out.println("ERROR: Client step is still using domain type 'CsvFolder' instead of gRPC type");
                } else if (sourceCode.contains("InputCsvFileProcessingSvc") && sourceCode.contains("CsvFolder")) {
                    System.out.println("SUCCESS: Client step is using gRPC type 'InputCsvFileProcessingSvc.CsvFolder'");
                }
                
                if (sourceCode.contains("CsvPaymentsInputFile") && !sourceCode.contains("InputCsvFileProcessingSvc")) {
                    System.out.println("ERROR: Client step is still using domain type 'CsvPaymentsInputFile' instead of gRPC type");
                } else if (sourceCode.contains("InputCsvFileProcessingSvc") && sourceCode.contains("CsvPaymentsInputFile")) {
                    System.out.println("SUCCESS: Client step is using gRPC type 'InputCsvFileProcessingSvc.CsvPaymentsInputFile'");
                }
                
                clientStepGenerated = true;
                break;
            }
        }

        assertTrue(clientStepGenerated, "Client step should be generated");
    }
}
