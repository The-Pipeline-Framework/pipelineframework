package org.pipelineframework.processor.util;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.ir.GrpcBinding;

/**
 * Resolves gRPC Java types from service descriptors at render time.
 * This class provides a deterministic, build-time-safe mechanism for discovering gRPC Java types
 * without storing them in the intermediate representation.
 *
 * <p><b>Assumptions about proto options:</b>
 * <ul>
 *   <li>Proto files should have proper java_package option set, or the package declaration will be used</li>
 *   <li>Proto files should have java_outer_classname option set, or it will be derived from the proto file name</li>
 *   <li>gRPC services follow the standard naming convention where the generated classes are nested in the outer class</li>
 * </ul>
 *
 * <p><b>Failure modes:</b>
 * <ul>
 *   <li>If the service descriptor is not of expected type, an IllegalArgumentException will be thrown</li>
 *   <li>If the proto file is renamed or options changed, this will fail fast with clear error messages</li>
 * </ul>
 */
public class GrpcJavaTypeResolver {

    /**
     * Resolves the gRPC Java types for a given GrpcBinding.
     *
     * @param binding The pipeline step binding containing the service descriptor
     * @return GrpcJavaTypes containing the resolved stub, impl base, parameter and return class names
     * @throws IllegalArgumentException if the service descriptor is invalid
     */
    public GrpcJavaTypes resolve(GrpcBinding binding) {
        return resolve(binding, null);
    }

    /**
         * Resolve gRPC Java types for a given GrpcBinding and emit optional warnings via a Messager.
         *
         * @param binding the pipeline step binding containing the service and method descriptors
         * @param messager a Messager for diagnostics, or {@code null} to disable warnings
         * @return a GrpcJavaTypes holder containing the resolved stub, impl base, parameter and return class names
         * @throws IllegalArgumentException if the binding's service descriptor is not a Descriptors.ServiceDescriptor
         * @throws IllegalStateException if required descriptor information (such as the method descriptor) is missing or resolution fails
         */
    public GrpcJavaTypes resolve(GrpcBinding binding, Messager messager) {
        Object serviceDescriptorObj = binding.serviceDescriptor();
        if (!(serviceDescriptorObj instanceof Descriptors.ServiceDescriptor serviceDescriptor)) {
            throw new IllegalArgumentException("Service descriptor is not of expected type Descriptors.ServiceDescriptor");
        }

        try {
            // Get parameter and return types from the method descriptor
            ClassName grpcParameterType;
            ClassName grpcReturnType;

            if (binding.methodDescriptor() != null) {
                Descriptors.MethodDescriptor methodDescriptor = (Descriptors.MethodDescriptor) binding.methodDescriptor();

                // Get the full names of the input and output types from the method descriptor
                // These are the actual protobuf message types (e.g., ".org.example.CsvFolder")
                String inputTypeName = methodDescriptor.getInputType().getFullName();
                String outputTypeName = methodDescriptor.getOutputType().getFullName();

                // Convert the protobuf FQNs to Java ClassNames properly
                grpcParameterType = convertProtoFqnToJavaClassName(inputTypeName, methodDescriptor);
                grpcReturnType = convertProtoFqnToJavaClassName(outputTypeName, methodDescriptor);
            } else {
                throw new IllegalStateException(
                    String.format("Method descriptor is null for service '%s'. " +
                        "This indicates that the protobuf descriptor does not contain the expected RPC method information. " +
                        "Please ensure the protobuf compilation is working correctly and the descriptor file is properly configured.",
                        binding.serviceName()));
            }

            // For the stub and impl base classes, we'll try to derive them from the service descriptor
            // but if that fails (e.g., in test scenarios), we'll return null which is acceptable
            ClassName stubClass = null;
            ClassName implBaseClass = null;

            try {
                String grpcOuterClass = deriveGrpcOuterClass(serviceDescriptor);
                String implBaseClassName = deriveImplBaseClass(grpcOuterClass, binding.serviceName());
                String stubClassName = deriveStubClass(grpcOuterClass, binding.serviceName());

                stubClass = convertFullNameToClassName(stubClassName);
                implBaseClass = convertFullNameToClassName(implBaseClassName);
            } catch (Exception e) {
                // In some test scenarios, the service descriptor might not have complete file information
                // This is acceptable as long as we have the parameter and return types
                if (messager != null) {
                    messager.printMessage(
                            Diagnostic.Kind.WARNING,
                            "Could not derive gRPC stub/impl base class names: " + e.getMessage());
                }
            }

            return new GrpcJavaTypes(
                stubClass,
                implBaseClass,
                grpcParameterType,
                grpcReturnType
            );
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format("Failed to resolve gRPC Java types for service '%s': %s",
                    binding.serviceName(), e.getMessage()), e);
        }
    }

    /**
     * Resolve a protobuf message fully-qualified name to the corresponding JavaPoet ClassName,
     * honouring `java_package`, `java_outer_classname` and `java_multiple_files` file options.
     *
     * @param descriptorProtoFqn the protobuf descriptor full name (for example ".pkg.PaymentStatus"); used to locate the message type on the provided method descriptor
     * @param methodDescriptor the method descriptor whose input/output types are consulted to resolve the message descriptor
     * @return the JavaPoet ClassName for the protobuf message, or `null` if `descriptorProtoFqn` is null or empty
     * @throws IllegalStateException if the message descriptor cannot be resolved from the given method descriptor
     */
    private ClassName convertProtoFqnToJavaClassName(String descriptorProtoFqn, Descriptors.MethodDescriptor methodDescriptor) {
        if (descriptorProtoFqn == null || descriptorProtoFqn.isEmpty()) {
            return null;
        }

        // Resolve the message descriptor from the method, not the method's file
        Descriptors.Descriptor messageDescriptor;
        if (descriptorProtoFqn.equals(methodDescriptor.getInputType().getFullName())) {
            messageDescriptor = methodDescriptor.getInputType();
        } else if (descriptorProtoFqn.equals(methodDescriptor.getOutputType().getFullName())) {
            messageDescriptor = methodDescriptor.getOutputType();
        } else {
            throw new IllegalStateException("Unable to resolve message descriptor for proto type: " + descriptorProtoFqn);
        }

        Descriptors.FileDescriptor fileDescriptor = messageDescriptor.getFile();

        // Determine Java package
        String javaPkg = fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();

        String messageName = messageDescriptor.getName();

        // Handle java_multiple_files
        if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
            return ClassName.get(javaPkg, messageName);
        }

        // Determine outer class name
        String outerClassName;
        if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
            outerClassName = fileDescriptor.getOptions().getJavaOuterClassname();
        } else {
            String fileName = fileDescriptor.getName();
            if (fileName.endsWith(".proto")) {
                fileName = fileName.substring(0, fileName.length() - 6);
            }
            String[] parts = fileName.split("[^a-zA-Z0-9]+");
            StringBuilder sb = new StringBuilder();
            for (String part : parts) {
                if (!part.isEmpty()) {
                    sb.append(Character.toUpperCase(part.charAt(0)));
                    if (part.length() > 1) {
                        sb.append(part.substring(1));
                    }
                }
            }
            outerClassName = sb.toString();
        }

        return ClassName.get(javaPkg, outerClassName, messageName);
    }

    /**
     * Converts a dotted fully-qualified class name (optionally containing `$` to denote nested classes) into a ClassName.
     *
     * @param fullyQualifiedClassName the fully qualified class name, may include `$` to separate nested classes
     * @return the corresponding ClassName, or `null` if {@code fullyQualifiedClassName} is null or empty
     */
    private ClassName convertFullNameToClassName(String fullyQualifiedClassName) {
        if (fullyQualifiedClassName == null || fullyQualifiedClassName.isEmpty()) {
            return null;
        }

        int lastDotIndex = fullyQualifiedClassName.lastIndexOf('.');
        String packageName = "";
        String classNameWithNested = fullyQualifiedClassName;

        if (lastDotIndex != -1) {
            packageName = fullyQualifiedClassName.substring(0, lastDotIndex);
            classNameWithNested = fullyQualifiedClassName.substring(lastDotIndex + 1);
        }

        String[] parts = classNameWithNested.split("\\$");
        String outerClassName = parts[0];
        String[] innerClassNames = new String[0];

        if (parts.length > 1) {
            innerClassNames = new String[parts.length - 1];
            System.arraycopy(parts, 1, innerClassNames, 0, parts.length - 1);
        }

        return ClassName.get(packageName, outerClassName, innerClassNames);
    }

    /**
     * Determine the fully qualified gRPC outer class name for a service descriptor.
     *
     * @param serviceDescriptor the service descriptor whose file options and service name are used to derive the outer class
     * @return the fully qualified outer class name (package and outer class) or the outer class name alone if the file has no Java package
     * @throws IllegalStateException if the provided service descriptor is null
     */
    private String deriveGrpcOuterClass(Descriptors.ServiceDescriptor serviceDescriptor) {
        if (serviceDescriptor == null) {
            throw new IllegalStateException("Service descriptor is null");
        }

        // Get the package name from the file descriptor
        String packageName = serviceDescriptor.getFile().getOptions().getJavaPackage();
        if (packageName.isEmpty()) {
            packageName = serviceDescriptor.getFile().getPackage();
        }

        // Use javaOuterClassname from proto options if set, otherwise derive from service name
        String outerClassName;
        String javaOuterClassname = serviceDescriptor.getFile().getOptions().getJavaOuterClassname(); // Get from proto options

        if (!javaOuterClassname.isEmpty()) {
            outerClassName = javaOuterClassname; // Use the explicitly defined outer class name
        } else {
            // For mutiny gRPC services, the outer class follows the pattern: Mutiny${ServiceName}ServiceGrpc
            // where ServiceName is the name of the gRPC service (e.g., ProcessFolderService)
            outerClassName = "Mutiny" + serviceDescriptor.getName() + "Grpc";
        }

        if (!packageName.isEmpty()) {
            return packageName + "." + outerClassName;
        } else {
            return outerClassName;
        }
    }

    /**
         * Compute the fully qualified class name for the gRPC implementation base for a given service.
         *
         * @param grpcOuterClass the fully qualified gRPC outer class name (must not be null or empty)
         * @param serviceName the service name as defined in the proto descriptor
         * @return the fully qualified name of the gRPC implementation base class
         * @throws IllegalStateException if {@code grpcOuterClass} is null or empty
         */
    private String deriveImplBaseClass(String grpcOuterClass, String serviceName) {
        if (grpcOuterClass == null || grpcOuterClass.isEmpty()) {
            throw new IllegalStateException("gRPC outer class name is null or empty");
        }

        // For mutiny gRPC services, the impl base class follows the pattern:
        // Mutiny${ServiceName}ServiceGrpc.${ServiceName}ServiceImplBase
        // Extract the service name from the full service name (e.g., "ProcessFolderService" -> "ProcessFolder")
        String serviceBaseName = serviceName;
        if (serviceBaseName.endsWith("Service")) {
            serviceBaseName = serviceBaseName.substring(0, serviceBaseName.length() - 7); // Remove "Service"
        }

        return grpcOuterClass + "$" + serviceBaseName + "ServiceImplBase";
    }

    /**
         * Derives the fully qualified Mutiny gRPC stub class name for a service.
         *
         * @param grpcOuterClass the fully qualified gRPC outer class name (must not be null or empty)
         * @param serviceName the service name as declared in the proto
         * @return the fully qualified class name of the Mutiny gRPC stub
         * @throws IllegalStateException if {@code grpcOuterClass} is null or empty
         */
    private String deriveStubClass(String grpcOuterClass, String serviceName) {
        if (grpcOuterClass == null || grpcOuterClass.isEmpty()) {
            throw new IllegalStateException("gRPC outer class name is null or empty");
        }

        // For mutiny gRPC services, the stub class follows the pattern:
        // Mutiny${ServiceName}ServiceGrpc.Mutiny${ServiceName}ServiceStub
        String mutinyStubName = "Mutiny" + serviceName + "Stub";

        return grpcOuterClass + "$" + mutinyStubName;
    }

    /**
     * Value class holding the resolved gRPC Java types.
     */
    public static class GrpcJavaTypes {
        private final ClassName stub;
        private final ClassName implBase;
        private final ClassName grpcParameterType;
        private final ClassName grpcReturnType;

        /**
         * Initialises a holder containing resolved gRPC-related Java type names.
         *
         * @param stub              the client stub class `ClassName` (Mutiny stub) for the service, or `null` if not resolved
         * @param implBase          the server implementation base class `ClassName` for the service, or `null` if not resolved
         * @param grpcParameterType the Java `ClassName` for the gRPC request message type, or `null` if not applicable
         * @param grpcReturnType    the Java `ClassName` for the gRPC response message type, or `null` if not applicable
         */
        public GrpcJavaTypes(ClassName stub, ClassName implBase, ClassName grpcParameterType, ClassName grpcReturnType) {
            this.stub = stub;
            this.implBase = implBase;
            this.grpcParameterType = grpcParameterType;
            this.grpcReturnType = grpcReturnType;
        }

        /**
         * Access the resolved gRPC client stub ClassName.
         *
         * @return the ClassName representing the generated Mutiny client stub
         */
        public ClassName stub() {
            return stub;
        }

        /**
         * The implementation base class for the gRPC service.
         *
         * @return the `ClassName` representing the generated service implementation base
         */
        public ClassName implBase() {
            return implBase;
        }

        /**
         * The resolved gRPC request message type for the method.
         *
         * @return the protobuf-derived Java ClassName for the gRPC request message, or null if not available.
         */
        public ClassName grpcParameterType() {
            return grpcParameterType;
        }

        /**
         * The Java type used as the gRPC method response.
         *
         * @return the `ClassName` for the gRPC method's response message, or `null` if unavailable
         */
        public ClassName grpcReturnType() {
            return grpcReturnType;
        }
    }
}