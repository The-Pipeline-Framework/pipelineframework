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
    private static final Set<String> ALLOWED_METHODS = Set.of(
        OrchestratorRpcConstants.RUN_METHOD,
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
        boolean allBuilt = false;
        int maxIterations = descriptorSet.getFileCount() * 2;
        int iterations = 0;

        while (!allBuilt && iterations < maxIterations) {
            allBuilt = true;
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
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Failed to build file descriptor for '" + fileName + "' while resolving " + serviceName,
                        e);
                }
            }
        }

        if (builtFileDescriptors.size() != descriptorSet.getFileCount()) {
            Set<String> builtFiles = builtFileDescriptors.keySet();
            Set<String> allFiles = new HashSet<>();
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                allFiles.add(fileProto.getName());
            }
            allFiles.removeAll(builtFiles);
            String unbuiltFiles = String.join(", ", allFiles);
            throw new IllegalStateException(
                "Could not resolve all file descriptor dependencies after " + iterations +
                    " iterations. Unbuilt files: [" + unbuiltFiles + "]");
        }

        return builtFileDescriptors;
    }

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

    private void validateStreamingSemantics(
        Descriptors.MethodDescriptor methodDescriptor,
        boolean inputStreaming,
        boolean outputStreaming
    ) {
        if (methodDescriptor.isClientStreaming() != inputStreaming ||
            methodDescriptor.isServerStreaming() != outputStreaming) {
            throw new IllegalStateException(
                "Orchestrator service streaming semantics do not match expected pipeline shape");
        }
    }
}
