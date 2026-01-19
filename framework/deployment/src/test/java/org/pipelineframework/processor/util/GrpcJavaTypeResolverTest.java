package org.pipelineframework.processor.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.*;

class GrpcJavaTypeResolverTest {

    private GrpcJavaTypeResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new GrpcJavaTypeResolver();
    }

    @Test
    void testResolveWithValidServiceDescriptor() throws Exception {
        InputStream in = getClass().getClassLoader().getResourceAsStream("descriptor_set.dsc");
        assertNotNull(in, "descriptor_set.dsc not found on classpath");

        DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(in);
        List<Descriptors.FileDescriptor> files = buildAllFileDescriptors(set);

        // 1) Assert descriptor sanity: service really exists
        Descriptors.ServiceDescriptor service = files.stream()
            .flatMap(fd -> fd.getServices().stream())
            .filter(sd -> sd.getName().equals("ProcessFolderService"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("ProcessFolderService not found in descriptor set"));

        Descriptors.MethodDescriptor method =
            service.findMethodByName("remoteProcess");

        assertNotNull(method, "remoteProcess method not found");

        // 2) Assert proto ownership (this is what actually broke before)
        assertEquals(
            "input_csv_file_processing_svc.proto",
            method.getInputType().getFile().getName(),
            "Input type must come from input_csv_file_processing_svc.proto"
        );

        assertEquals(
            "input_csv_file_processing_svc.proto",
            method.getOutputType().getFile().getName(),
            "Output type must come from input_csv_file_processing_svc.proto"
        );

        // 3) Resolve Java types
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(service.getName())
            .servicePackage(service.getFile().getPackage())
            .serviceClassName(ClassName.get("org.pipelineframework.csv.grpc", service.getName()))
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.domain", "CsvFolder"),
                null,
                false))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.domain", "CsvPaymentsInputFile"),
                null,
                false))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
            .addEnabledTarget(GenerationTarget.CLIENT_STEP)
            .build();

        GrpcBinding binding = new GrpcBinding.Builder()
            .model(model)
            .serviceDescriptor(service)
            .methodDescriptor(method)
            .build();

        GrpcJavaTypeResolver.GrpcJavaTypes types = resolver.resolve(binding);

        // 4) Assert final Java resolution (the real invariant)
        assertEquals(
            "org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc.CsvFolder",
            types.grpcParameterType().toString()
        );

        assertEquals(
            "org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc.CsvPaymentsInputFile",
            types.grpcReturnType().toString()
        );
    }

    private List<Descriptors.FileDescriptor> buildAllFileDescriptors(DescriptorProtos.FileDescriptorSet set) {

        Map<String, DescriptorProtos.FileDescriptorProto> protoByName = new HashMap<>();
        for (DescriptorProtos.FileDescriptorProto f : set.getFileList()) {
            protoByName.put(f.getName(), f);
        }

        Map<String, Descriptors.FileDescriptor> resolved = new HashMap<>();

        Function<DescriptorProtos.FileDescriptorProto, Descriptors.FileDescriptor> resolve =
            new Function<>() {
                @Override
                public Descriptors.FileDescriptor apply(DescriptorProtos.FileDescriptorProto proto) {
                    return resolved.computeIfAbsent(proto.getName(), name -> {
                        try {
                            Descriptors.FileDescriptor[] deps = proto.getDependencyList().stream()
                                .map(dep -> apply(protoByName.get(dep)))
                                .toArray(Descriptors.FileDescriptor[]::new);

                            return Descriptors.FileDescriptor.buildFrom(proto, deps);
                        } catch (Descriptors.DescriptorValidationException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            };

        for (DescriptorProtos.FileDescriptorProto proto : set.getFileList()) {
            resolve.apply(proto);
        }

        return List.copyOf(resolved.values());
    }

    @Test
    void testResolveWhenServiceDescriptorIsNull() {
        // Create a mock PipelineStepModel
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("TestService")
            .servicePackage("com.example.test")
            .serviceClassName(ClassName.get("com.example.test", "TestService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.domain", "InputDomain"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.domain", "OutputDomain"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
            .addEnabledTarget(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE)
            .build();

        // Create a GrpcBinding with null service descriptor
        GrpcBinding binding = new GrpcBinding.Builder()
            .model(model)
            .serviceDescriptor(null)  // This should cause an exception
            .build();

        // Test that an exception is thrown
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> resolver.resolve(binding));

        assertTrue(exception.getMessage().contains("Service descriptor is not of expected type Descriptors.ServiceDescriptor"));
    }

    @Test
    void testResolveWhenServiceDescriptorIsWrongType() {
        // Create a mock PipelineStepModel
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("TestService")
            .servicePackage("com.example.test")
            .serviceClassName(ClassName.get("com.example.test", "TestService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.domain", "InputDomain"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.domain", "OutputDomain"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
            .addEnabledTarget(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE)
            .build();

        // Create a GrpcBinding with wrong type service descriptor
        GrpcBinding binding = new GrpcBinding.Builder()
            .model(model)
            .serviceDescriptor("This is not a ServiceDescriptor")  // Wrong type
            .build();

        // Test that an exception is thrown
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> resolver.resolve(binding));

        assertTrue(exception.getMessage().contains("Service descriptor is not of expected type Descriptors.ServiceDescriptor"));
    }
}
