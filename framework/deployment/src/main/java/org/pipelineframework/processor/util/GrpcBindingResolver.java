package org.pipelineframework.processor.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Resolves gRPC bindings exclusively using compiled protobuf descriptors (FileDescriptorSet) and strict naming conventions.
 * This class creates GrpcBinding objects without storing gRPC Java types in the intermediate representation,
 * as those are now resolved at render time by the GrpcJavaTypeResolver.
 */
public class GrpcBindingResolver {

    /**
     * Default constructor for GrpcBindingResolver.
     */
    public GrpcBindingResolver() {
    }

    /**
     * Resolves gRPC bindings for a given PipelineStepModel using the provided FileDescriptorSet.
     *
     * @param stepModel The semantic pipeline step definition
     * @param descriptorSet The compiled protobuf descriptors
     * @return A fully populated GrpcBinding
     * @throws IllegalStateException if validation fails or descriptorSet is null
     */
    public GrpcBinding resolve(PipelineStepModel stepModel, DescriptorProtos.FileDescriptorSet descriptorSet) {
        if (descriptorSet == null) {
            throw new IllegalStateException(
                String.format("FileDescriptorSet is null for service '%s'. " +
                    "This indicates that the protobuf compilation did not produce descriptor files " +
                    "or the descriptor file location is not properly configured. " +
                    "Please ensure protobuf compilation happens before annotation processing.",
                    stepModel.serviceName()));
        }

        Descriptors.ServiceDescriptor serviceDescriptor;
        Descriptors.MethodDescriptor methodDescriptor;

        try {
            // Locate the ServiceDescriptorProto whose name matches stepModel.getServiceName()
            serviceDescriptor = findServiceDescriptor(stepModel, descriptorSet);

            // Locate the RPC method named remoteProcess
            methodDescriptor = findRemoteProcessMethod(serviceDescriptor, stepModel);

            // Validate streaming semantics
            validateStreamingSemantics(methodDescriptor, stepModel);

            // Validate type mappings
            validateTypeMappings(methodDescriptor, stepModel);
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format("Failed to resolve gRPC binding for service '%s': %s",
                    stepModel.serviceName(), e.getMessage()), e);
        }

        // Create and populate GrpcBinding using the builder with resolved descriptors
        return new GrpcBinding.Builder()
                .model(stepModel)
                .serviceDescriptor(serviceDescriptor)
                .methodDescriptor(methodDescriptor)
                .build();
    }

    private Descriptors.ServiceDescriptor findServiceDescriptor(PipelineStepModel stepModel, DescriptorProtos.FileDescriptorSet descriptorSet) {
        String expectedServiceName = stepModel.serviceName();

        // Validate service name starts with Process
        if (!expectedServiceName.startsWith("Process")) {
            throw new IllegalStateException(
                String.format("Build error for step '%s': Service name '%s' must start with 'Process'",
                    stepModel.serviceName(), expectedServiceName));
        }

        // Build all file descriptors with proper dependency resolution
        // This handles complex dependency scenarios including imported files and transitive dependencies
        java.util.Map<String, Descriptors.FileDescriptor> builtFileDescriptors = new java.util.HashMap<>();

        // Create a mapping from file names to their FileDescriptorProto
        java.util.Map<String, DescriptorProtos.FileDescriptorProto> protoMap = new java.util.HashMap<>();
        for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
            protoMap.put(fileProto.getName(), fileProto);
        }

        // Build file descriptors in dependency order using topological sort approach
        boolean allBuilt = false;
        int maxIterations = descriptorSet.getFileCount() * 2; // Prevent infinite loops
        int iterations = 0;

        while (!allBuilt && iterations < maxIterations) {
            allBuilt = true;
            iterations++;

            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                String fileName = fileProto.getName();

                // Skip if already built
                if (builtFileDescriptors.containsKey(fileName)) {
                    continue;
                }

                // Check if all dependencies are already built
                boolean allDependenciesBuilt = true;
                for (String dependencyName : fileProto.getDependencyList()) {
                    if (!builtFileDescriptors.containsKey(dependencyName)) {
                        allDependenciesBuilt = false;
                        break;
                    }
                }

                if (allDependenciesBuilt) {
                    // Build this file descriptor with its dependencies
                    java.util.List<Descriptors.FileDescriptor> dependencies = new java.util.ArrayList<>();
                    for (String dependencyName : fileProto.getDependencyList()) {
                        dependencies.add(builtFileDescriptors.get(dependencyName));
                    }

                    try {
                        Descriptors.FileDescriptor[] depsArray = dependencies.toArray(new Descriptors.FileDescriptor[0]);
                        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, depsArray);
                        builtFileDescriptors.put(fileName, fileDescriptor);
                    } catch (Exception e) {
                        throw new IllegalStateException(
                            String.format("Build error for step '%s': Failed to build file descriptor for '%s': %s",
                                stepModel.serviceName(), fileName, e.getMessage()), e);
                    }
                } else {
                    allBuilt = false; // Need another iteration
                }
            }
        }

        // Check if all files were built
        if (builtFileDescriptors.size() != descriptorSet.getFileCount()) {
            // Identify which files couldn't be built
            java.util.Set<String> builtFiles = builtFileDescriptors.keySet();
            java.util.Set<String> allFiles = new java.util.HashSet<>();
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                allFiles.add(fileProto.getName());
            }

            allFiles.removeAll(builtFiles);
            String unbuiltFiles = String.join(", ", allFiles);

            throw new IllegalStateException(
                String.format("Build error for step '%s': Could not resolve all file descriptor dependencies after %d iterations. Built: %d, Expected: %d. Unbuilt files: [%s]",
                    stepModel.serviceName(), iterations, builtFileDescriptors.size(), descriptorSet.getFileCount(), unbuiltFiles));
        }

        // Now look for the service in all built file descriptors
        Descriptors.ServiceDescriptor foundService = null;
        for (Descriptors.FileDescriptor fileDescriptor : builtFileDescriptors.values()) {
            for (Descriptors.ServiceDescriptor service : fileDescriptor.getServices()) {
                if (service.getName().equals(expectedServiceName)) {
                    if (foundService != null) {
                        throw new IllegalStateException(
                            String.format("Build error for step '%s': Multiple services named '%s' found in descriptor set",
                                stepModel.serviceName(), expectedServiceName));
                    }
                    foundService = service;
                }
            }
        }

        if (foundService == null) {
            throw new IllegalStateException(
                String.format("Build error for step '%s': Service named '%s' not found in descriptor set",
                    stepModel.serviceName(), expectedServiceName));
        }

        return foundService;
    }

    private Descriptors.MethodDescriptor findRemoteProcessMethod(Descriptors.ServiceDescriptor serviceDescriptor, PipelineStepModel stepModel) {
        String expectedMethodName = "remoteProcess";
        Descriptors.MethodDescriptor foundMethod = null;

        for (Descriptors.MethodDescriptor method : serviceDescriptor.getMethods()) {
            if (method.getName().equals(expectedMethodName)) {
                if (foundMethod != null) {
                    throw new IllegalStateException(
                        String.format("Build error for step '%s': Multiple methods named '%s' found in service '%s'",
                            stepModel.serviceName(), expectedMethodName, serviceDescriptor.getName()));
                }
                foundMethod = method;
            }
        }

        if (foundMethod == null) {
            throw new IllegalStateException(
                String.format("Build error for step '%s': Method '%s' not found in service '%s'",
                    stepModel.serviceName(), expectedMethodName, serviceDescriptor.getName()));
        }

        // Check if there are other methods and emit warning if so
        if (serviceDescriptor.getMethods().size() > 1) {
            System.out.println(
                String.format("Warning for step '%s': Service '%s' contains multiple RPC methods. Only '%s' will be bound.",
                    stepModel.serviceName(), serviceDescriptor.getName(), expectedMethodName));
        }

        return foundMethod;
    }

    private void validateStreamingSemantics(Descriptors.MethodDescriptor methodDescriptor, PipelineStepModel stepModel) {
        StreamingShape expectedShape = stepModel.streamingShape();
        boolean isRequestStreaming = methodDescriptor.isClientStreaming();
        boolean isResponseStreaming = methodDescriptor.isServerStreaming();

        StreamingShape actualShape;
        if (!isRequestStreaming && !isResponseStreaming) {
            actualShape = StreamingShape.UNARY_UNARY;
        } else if (isRequestStreaming && !isResponseStreaming) {
            actualShape = StreamingShape.STREAMING_UNARY;  // Client streaming
        } else if (!isRequestStreaming && isResponseStreaming) {
            actualShape = StreamingShape.UNARY_STREAMING;  // Server streaming
        } else {
            actualShape = StreamingShape.STREAMING_STREAMING;  // Bidirectional streaming
        }

        if (actualShape != expectedShape) {
            throw new IllegalStateException(
                String.format("Build error for step '%s': Expected streaming shape '%s' but found '%s' for method '%s'",
                    stepModel.serviceName(), expectedShape, actualShape, methodDescriptor.getName()));
        }
    }

    private void validateTypeMappings(Descriptors.MethodDescriptor methodDescriptor, PipelineStepModel stepModel) {
        // Type mappings are resolved at render time via GrpcJavaTypeResolver.
    }
}
