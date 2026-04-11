package org.pipelineframework.processor.util;

import java.util.*;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Resolves gRPC bindings for the orchestrator service using compiled protobuf descriptors.
 */
public class OrchestratorGrpcBindingResolver {
    private static final String ANY_PROTO_PATH = "google/protobuf/any.proto";
    private static final String API_PROTO_PATH = "google/protobuf/api.proto";
    private static final String DESCRIPTOR_PROTO_PATH = "google/protobuf/descriptor.proto";
    private static final String DURATION_PROTO_PATH = "google/protobuf/duration.proto";
    private static final String EMPTY_PROTO_PATH = "google/protobuf/empty.proto";
    private static final String FIELD_MASK_PROTO_PATH = "google/protobuf/field_mask.proto";
    private static final String SOURCE_CONTEXT_PROTO_PATH = "google/protobuf/source_context.proto";
    private static final String STRUCT_PROTO_PATH = "google/protobuf/struct.proto";
    private static final String TIMESTAMP_PROTO_PATH = "google/protobuf/timestamp.proto";
    private static final String TYPE_PROTO_PATH = "google/protobuf/type.proto";
    private static final String WRAPPERS_PROTO_PATH = "google/protobuf/wrappers.proto";
    private static final Set<String> ALLOWED_METHODS = Set.of(
        OrchestratorRpcConstants.RUN_METHOD,
        OrchestratorRpcConstants.RUN_ASYNC_METHOD,
        OrchestratorRpcConstants.GET_EXECUTION_STATUS_METHOD,
        OrchestratorRpcConstants.GET_EXECUTION_RESULT_METHOD,
        OrchestratorRpcConstants.INGEST_METHOD,
        OrchestratorRpcConstants.SUBSCRIBE_METHOD);

    /**
     * Creates a new OrchestratorGrpcBindingResolver.
     */
    public OrchestratorGrpcBindingResolver() {
    }

    /**
     * Resolve the orchestrator gRPC binding using the descriptor set.
     *
     * @param model the synthetic model describing the orchestrator service
     * @param descriptorSet the compiled protobuf descriptor set
     * @param methodName the expected RPC method name
     * @param inputStreaming whether the method should be client streaming
     * @param outputStreaming whether the method should be server streaming
     * @param messager optional messager for warnings
     * @return the resolved GrpcBinding
     */
    public GrpcBinding resolve(
        PipelineStepModel model,
        DescriptorProtos.FileDescriptorSet descriptorSet,
        String methodName,
        boolean inputStreaming,
        boolean outputStreaming,
        Messager messager
    ) {
        if (descriptorSet == null) {
            throw new IllegalStateException(
                "FileDescriptorSet is null for orchestrator service. " +
                    "Ensure protobuf compilation runs before annotation processing.");
        }

        Descriptors.ServiceDescriptor serviceDescriptor = findServiceDescriptor(model.serviceName(), descriptorSet);
        Descriptors.MethodDescriptor methodDescriptor = findMethodDescriptor(serviceDescriptor, methodName, messager);
        validateStreamingSemantics(methodDescriptor, inputStreaming, outputStreaming);

        return new GrpcBinding.Builder()
            .model(model)
            .serviceDescriptor(serviceDescriptor)
            .methodDescriptor(methodDescriptor)
            .build();
    }

    private Descriptors.ServiceDescriptor findServiceDescriptor(
        String serviceName,
        DescriptorProtos.FileDescriptorSet descriptorSet
    ) {
        Map<String, Descriptors.FileDescriptor> builtFileDescriptors = buildFileDescriptors(descriptorSet, serviceName);

        Descriptors.ServiceDescriptor foundService = null;
        for (Descriptors.FileDescriptor fileDescriptor : builtFileDescriptors.values()) {
            for (Descriptors.ServiceDescriptor service : fileDescriptor.getServices()) {
                if (service.getName().equals(serviceName)) {
                    if (foundService != null) {
                        throw new IllegalStateException(
                            "Multiple services named '" + serviceName + "' found in descriptor set");
                    }
                    foundService = service;
                }
            }
        }

        if (foundService == null) {
            throw new IllegalStateException(
                "Service named '" + serviceName + "' not found in descriptor set");
        }

        return foundService;
    }

    private Map<String, Descriptors.FileDescriptor> buildFileDescriptors(
        DescriptorProtos.FileDescriptorSet descriptorSet,
        String serviceName
    ) {
        Map<String, Descriptors.FileDescriptor> builtFileDescriptors = new HashMap<>();
        seedWellKnownDescriptors(builtFileDescriptors);
        Set<String> projectFileNames = new HashSet<>();
        for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
            projectFileNames.add(fileProto.getName());
        }
        int builtProjectCount = (int) projectFileNames.stream()
            .filter(builtFileDescriptors::containsKey)
            .count();
        int maxIterations = descriptorSet.getFileCount() * 2;
        int iterations = 0;

        while (builtProjectCount < projectFileNames.size() && iterations < maxIterations) {
            boolean allBuilt = true;
            iterations++;

            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                String fileName = fileProto.getName();
                if (builtFileDescriptors.containsKey(fileName)) {
                    continue;
                }

                boolean allDependenciesBuilt = true;
                for (String dependencyName : fileProto.getDependencyList()) {
                    if (!builtFileDescriptors.containsKey(dependencyName)) {
                        allDependenciesBuilt = false;
                        break;
                    }
                }

                if (!allDependenciesBuilt) {
                    allBuilt = false;
                    continue;
                }

                List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
                for (String dependencyName : fileProto.getDependencyList()) {
                    dependencies.add(builtFileDescriptors.get(dependencyName));
                }

                try {
                    Descriptors.FileDescriptor[] depsArray = dependencies.toArray(new Descriptors.FileDescriptor[0]);
                    Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, depsArray);
                    builtFileDescriptors.put(fileName, fileDescriptor);
                    if (projectFileNames.contains(fileName)) {
                        builtProjectCount++;
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Failed to build file descriptor for '" + fileName + "' while resolving " + serviceName,
                        e);
                }
            }

            if (allBuilt) {
                break;
            }
        }

        if (builtProjectCount != projectFileNames.size()) {
            Set<String> allFiles = new HashSet<>(projectFileNames);
            allFiles.removeAll(builtFileDescriptors.keySet());
            String unbuiltFiles = String.join(", ", allFiles);
            throw new IllegalStateException(
                "Could not resolve all file descriptor dependencies after " + iterations +
                    " iterations. Unbuilt files: [" + unbuiltFiles + "]");
        }

        return builtFileDescriptors;
    }

    private void seedWellKnownDescriptors(Map<String, Descriptors.FileDescriptor> builtFileDescriptors) {
        builtFileDescriptors.putIfAbsent(ANY_PROTO_PATH, com.google.protobuf.AnyProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(API_PROTO_PATH, com.google.protobuf.ApiProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(DESCRIPTOR_PROTO_PATH, com.google.protobuf.DescriptorProtos.getDescriptor());
        builtFileDescriptors.putIfAbsent(DURATION_PROTO_PATH, com.google.protobuf.DurationProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(EMPTY_PROTO_PATH, com.google.protobuf.EmptyProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(FIELD_MASK_PROTO_PATH, com.google.protobuf.FieldMaskProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(SOURCE_CONTEXT_PROTO_PATH, com.google.protobuf.SourceContextProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(STRUCT_PROTO_PATH, com.google.protobuf.StructProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(TIMESTAMP_PROTO_PATH, com.google.protobuf.TimestampProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(TYPE_PROTO_PATH, com.google.protobuf.TypeProto.getDescriptor());
        builtFileDescriptors.putIfAbsent(WRAPPERS_PROTO_PATH, com.google.protobuf.WrappersProto.getDescriptor());
    }

    /**
     * Finds the RPC method descriptor with the given name in the service descriptor.
     *
     * @param serviceDescriptor the service descriptor to search
     * @param methodName the RPC method name to locate
     * @param messager optional Messager to emit a warning if the service exposes RPCs outside the allowed set; may be null
     * @return the matching {@link Descriptors.MethodDescriptor}
     * @throws IllegalStateException if no method with the given name is found or if multiple methods share the same name
     */
    private Descriptors.MethodDescriptor findMethodDescriptor(
        Descriptors.ServiceDescriptor serviceDescriptor,
        String methodName,
        Messager messager
    ) {
        Descriptors.MethodDescriptor found = null;
        for (Descriptors.MethodDescriptor method : serviceDescriptor.getMethods()) {
            if (method.getName().equals(methodName)) {
                if (found != null) {
                    throw new IllegalStateException(
                        "Multiple methods named '" + methodName + "' found in orchestrator service");
                }
                found = method;
            }
        }
        if (found == null) {
            throw new IllegalStateException(
                "Method '" + methodName + "' not found in orchestrator service");
        }
        if (serviceDescriptor.getMethods().size() > 1 && messager != null) {
            boolean hasUnexpected = serviceDescriptor.getMethods().stream()
                .map(Descriptors.MethodDescriptor::getName)
                .anyMatch(name -> !ALLOWED_METHODS.contains(name));
            if (hasUnexpected) {
                messager.printMessage(
                    Diagnostic.Kind.WARNING,
                    "Multiple RPCs found in orchestrator service; only '" + methodName + "' is used.");
            }
        }
        return found;
    }

    /**
     * Validates that the method's client and server streaming flags match the expected pipeline shape.
     *
     * @param methodDescriptor the gRPC method descriptor to validate
     * @param inputStreaming   true if the pipeline expects client (input) streaming
     * @param outputStreaming  true if the pipeline expects server (output) streaming
     * @throws IllegalStateException if the method's clientStreaming or serverStreaming flag differs from the corresponding expected value
     */
    private void validateStreamingSemantics(
        Descriptors.MethodDescriptor methodDescriptor,
        boolean inputStreaming,
        boolean outputStreaming
    ) {
        boolean actualClientStreaming = methodDescriptor.isClientStreaming();
        boolean actualServerStreaming = methodDescriptor.isServerStreaming();
        if (actualClientStreaming != inputStreaming || actualServerStreaming != outputStreaming) {
            String methodIdentity = methodDescriptor.getFullName();
            throw new IllegalStateException(
                "Orchestrator service streaming semantics mismatch for method '" + methodIdentity + "': " +
                    "clientStreaming expected=" + inputStreaming + " actual=" + actualClientStreaming + ", " +
                    "serverStreaming expected=" + outputStreaming + " actual=" + actualServerStreaming);
        }
    }
}
