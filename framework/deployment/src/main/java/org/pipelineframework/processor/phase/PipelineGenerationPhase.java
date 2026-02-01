package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import org.jboss.logging.Logger;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.renderer.*;
import org.pipelineframework.processor.util.OrchestratorClientPropertiesGenerator;
import org.pipelineframework.processor.util.PipelineOrderMetadataGenerator;
import org.pipelineframework.processor.util.PipelineTelemetryMetadataGenerator;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

/**
 * Generates artifacts by iterating over GenerationTargets and delegating to PipelineRenderer implementations.
 * This phase reads semantic models, bindings, and GenerationContext from the compilation context
 * and delegates to the appropriate renderers.
 * <p>
 * This phase contains no JavaPoet logic, no decisions, and no binding construction.
 */
public class PipelineGenerationPhase implements PipelineCompilationPhase {

    private static final Logger LOG = Logger.getLogger(PipelineGenerationPhase.class);

    /**
     * Creates a new PipelineGenerationPhase.
     */
    public PipelineGenerationPhase() {
    }

    @Override
    public String name() {
        return "Pipeline Generation Phase";
    }

    /**
     * Orchestrates generation of pipeline source artifacts and metadata for all step models and the orchestrator.
     *
     * <p>Initializes renderers and generators, produces per-step artifacts (gRPC, REST, clients, side-effect beans),
     * optionally generates protobuf parsers when gRPC transport and a descriptor set are available, and writes role
     * and orchestrator-related metadata files.</p>
     *
     * @param ctx the compilation context containing step models, renderer bindings, descriptor set, environment and settings
     * @throws Exception if an unrecoverable generation error occurs that cannot be handled locally
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Only proceed if there are models to process or orchestrator to generate
        if (ctx.getStepModels().isEmpty() && !ctx.isOrchestratorGenerated()) {
            // Still need to write role metadata even if no models to process
            RoleMetadataGenerator roleMetadataGenerator = new RoleMetadataGenerator(ctx.getProcessingEnv());
            try {
                roleMetadataGenerator.writeRoleMetadata();
            } catch (Exception e) {
                // Log the error but don't fail the entire compilation
                // This can happen in test environments where the Filer doesn't properly create files
                if (ctx.getProcessingEnv() != null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.WARNING,
                        "Failed to write role metadata: " + e.getMessage());
                }
            }
            return;
        }

        // Get the bindings map from the context
        Map<String, Object> bindingsMap = ctx.getRendererBindings();

        // Initialize renderers
        GrpcServiceAdapterRenderer grpcRenderer = new GrpcServiceAdapterRenderer(GenerationTarget.GRPC_SERVICE);
        org.pipelineframework.processor.renderer.ClientStepRenderer clientRenderer =
            new org.pipelineframework.processor.renderer.ClientStepRenderer(GenerationTarget.CLIENT_STEP);
        RestClientStepRenderer restClientRenderer = new RestClientStepRenderer();
        RestResourceRenderer restRenderer = new RestResourceRenderer();
        OrchestratorGrpcRenderer orchestratorGrpcRenderer = new OrchestratorGrpcRenderer();
        OrchestratorRestResourceRenderer orchestratorRestRenderer = new OrchestratorRestResourceRenderer();
        OrchestratorCliRenderer orchestratorCliRenderer = new OrchestratorCliRenderer();

        // Initialize role metadata generator
        RoleMetadataGenerator roleMetadataGenerator = new RoleMetadataGenerator(ctx.getProcessingEnv());

        // Get the cache key generator
        ClassName cacheKeyGenerator = resolveCacheKeyGenerator(ctx);

        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();

        // Generate artifacts for each step model
        for (PipelineStepModel model : ctx.getStepModels()) {
            // Get the bindings for this model
            GrpcBinding grpcBinding = (GrpcBinding) bindingsMap.get(model.serviceName() + "_grpc");
            RestBinding restBinding = (RestBinding) bindingsMap.get(model.serviceName() + "_rest");

            // Generate artifacts based on enabled targets
            generateArtifacts(
                ctx,
                model,
                grpcBinding,
                restBinding,
                descriptorSet,
                cacheKeyGenerator,
                roleMetadataGenerator,
                grpcRenderer,
                clientRenderer,
                restClientRenderer,
                restRenderer
            );
        }

        if (ctx.isTransportModeGrpc() && descriptorSet != null) {
            generateProtobufParsers(ctx, descriptorSet);
        }

        // Generate orchestrator artifacts if needed
        if (ctx.isOrchestratorGenerated()) {
            OrchestratorBinding orchestratorBinding = (OrchestratorBinding) bindingsMap.get("orchestrator");
            if (orchestratorBinding != null) {
                generateOrchestratorServer(
                    ctx,
                    descriptorSet,
                    orchestratorBinding.cliName() != null, // Using cliName as indicator for CLI generation
                    orchestratorGrpcRenderer,
                    orchestratorRestRenderer,
                    orchestratorCliRenderer,
                    roleMetadataGenerator,
                    cacheKeyGenerator
                );
            }
        }

        // Write role metadata
        try {
            roleMetadataGenerator.writeRoleMetadata();
            if (ctx.isOrchestratorGenerated()) {
                PipelineOrderMetadataGenerator orderMetadataGenerator =
                    new PipelineOrderMetadataGenerator(ctx.getProcessingEnv());
                orderMetadataGenerator.writeOrderMetadata(ctx);
                PipelineTelemetryMetadataGenerator telemetryMetadataGenerator =
                    new PipelineTelemetryMetadataGenerator(ctx.getProcessingEnv());
                telemetryMetadataGenerator.writeTelemetryMetadata(ctx);
                OrchestratorClientPropertiesGenerator clientPropertiesGenerator =
                    new OrchestratorClientPropertiesGenerator(ctx.getProcessingEnv());
                clientPropertiesGenerator.writeClientProperties(ctx);
            }
        } catch (Exception e) {
            // Log the error but don't fail the entire compilation
            // This can happen in test environments where the Filer doesn't properly create files
            // or when the file is already opened by another process
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to write role metadata: " + e.getMessage());
            }
        }
    }

    /**
     * Generate Java protobuf message parser classes from the provided descriptor set into the
     * generated-sources directory for the appropriate deployment role.
     *
     * Builds file descriptors from the given FileDescriptorSet and, if any are produced,
     * determines the deployment role (PLUGIN_SERVER when the context is a plugin host,
     * PIPELINE_SERVER otherwise) and resolves that role's output directory. For each protobuf
     * message type found, generates a corresponding parser class (including nested message types)
     * and writes it to the role's generated sources directory; if no file descriptors can be
     * built, the method is a no-op.
     *
     * @param ctx the pipeline compilation context used to determine role and output locations
     * @param descriptorSet the protobuf FileDescriptorSet describing message types to generate parsers for
     */
    private void generateProtobufParsers(
            PipelineCompilationContext ctx,
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        Map<String, Descriptors.FileDescriptor> fileDescriptors = buildFileDescriptors(descriptorSet);
        if (fileDescriptors.isEmpty()) {
            return;
        }
        org.pipelineframework.processor.ir.DeploymentRole role = ctx.isPluginHost()
            ? org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER
            : org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER;
        java.nio.file.Path outputDir = resolveRoleOutputDir(ctx, role);
        Set<String> generated = new HashSet<>();

        for (Descriptors.FileDescriptor fileDescriptor : fileDescriptors.values()) {
            for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
                collectAndGenerateParser(ctx, descriptor, outputDir, generated);
            }
        }
    }

    /**
     * Generate and write a Java protobuf parser class for the given message descriptor and its nested types.
     *
     * <p>Skips descriptors marked as map entries or those that cannot be resolved to a Java message class.
     * When a parser is generated its fully-qualified class name is recorded in {@code generated} to avoid
     * duplicate generation. On I/O failure a warning is emitted via the processing environment messager if available.
     *
     * @param ctx the pipeline compilation context (used for writing files and reporting warnings)
     * @param descriptor the protobuf message descriptor to generate a parser for; nested types are processed recursively
     * @param outputDir the directory where the generated Java file should be written
     * @param generated a set of fully-qualified parser class names already generated; new names are added to this set
     */
    private void collectAndGenerateParser(
            PipelineCompilationContext ctx,
            Descriptors.Descriptor descriptor,
            java.nio.file.Path outputDir,
            Set<String> generated) {
        if (descriptor == null) {
            return;
        }
        if (!descriptor.getOptions().getMapEntry()) {
            ClassName messageType = resolveMessageClassName(descriptor);
            if (messageType != null) {
                String parserPackage = messageType.packageName().isBlank()
                    ? "pipeline"
                    : messageType.packageName() + ".pipeline";
                String parserName = "Proto" + String.join("_", messageType.simpleNames()) + "Parser";
                String fqcn = parserPackage + "." + parserName;
                if (generated.add(fqcn)) {
                    TypeSpec parserClass = buildParserClass(messageType, parserName);
                    try {
                        JavaFile.builder(parserPackage, parserClass).build().writeTo(outputDir);
                    } catch (IOException e) {
                        if (ctx.getProcessingEnv() != null) {
                            ctx.getProcessingEnv().getMessager().printMessage(
                                javax.tools.Diagnostic.Kind.WARNING,
                                "Failed to generate protobuf parser for '" + messageType + "': " + e.getMessage());
                        }
                    }
                }
            }
        }
        for (Descriptors.Descriptor nested : descriptor.getNestedTypes()) {
            collectAndGenerateParser(ctx, nested, outputDir, generated);
        }
    }

    /**
     * Builds a JavaPoet TypeSpec for a Protobuf parser class.
     *
     * The generated class is public, annotated with `@ApplicationScoped` and `@Unremovable`,
     * implements `org.pipelineframework.cache.ProtobufMessageParser`, exposes a `type()`
     * method that returns the message type's canonical name, and a `parseFrom(byte[])`
     * method that delegates to the Protobuf message's `parseFrom` and wraps any
     * `InvalidProtocolBufferException` in a `RuntimeException`.
     *
     * @param messageType the Protobuf message Java type (as a ClassName) that the parser will handle
     * @param parserName  the simple class name to use for the generated parser
     * @return a TypeSpec describing the generated parser class
     */
    private TypeSpec buildParserClass(ClassName messageType, String parserName) {
        ClassName parserInterface = ClassName.get("org.pipelineframework.cache", "ProtobufMessageParser");
        ClassName messageBase = ClassName.get("com.google.protobuf", "Message");
        ClassName invalidProto = ClassName.get("com.google.protobuf", "InvalidProtocolBufferException");

        MethodSpec typeMethod = MethodSpec.methodBuilder("type")
            .addAnnotation(Override.class)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $S", messageType.toString())
            .build();

        MethodSpec parseMethod = MethodSpec.methodBuilder("parseFrom")
            .addAnnotation(Override.class)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .returns(messageBase)
            .addParameter(byte[].class, "bytes")
            .addCode("""
                try {
                    return $T.parseFrom(bytes);
                } catch ($T e) {
                    throw new RuntimeException("Failed to parse " + type(), e);
                }
                """,
                messageType,
                invalidProto)
            .build();

        return TypeSpec.classBuilder(parserName)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
            .addAnnotation(ClassName.get("io.quarkus.arc", "Unremovable"))
            .addSuperinterface(parserInterface)
            .addMethod(typeMethod)
            .addMethod(parseMethod)
            .build();
    }

    /**
     * Builds FileDescriptor instances from a protobuf FileDescriptorSet, resolving inter-file dependencies.
     *
     * <p>Files whose dependencies cannot be resolved or whose descriptors fail validation are omitted.</p>
     *
     * @param descriptorSet the protobuf FileDescriptorSet to convert (may be null)
     * @return a map keyed by proto file name to its resolved {@link Descriptors.FileDescriptor}; empty if the input is null
     */
    private Map<String, Descriptors.FileDescriptor> buildFileDescriptors(
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
        if (descriptorSet == null) {
            return built;
        }
        boolean progress = true;
        while (built.size() < descriptorSet.getFileCount() && progress) {
            progress = false;
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                String fileName = fileProto.getName();
                if (built.containsKey(fileName)) {
                    continue;
                }
                boolean depsReady = true;
                for (String dependency : fileProto.getDependencyList()) {
                    if (!built.containsKey(dependency)) {
                        depsReady = false;
                        break;
                    }
                }
                if (!depsReady) {
                    continue;
                }
                try {
                    List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
                    for (String dependency : fileProto.getDependencyList()) {
                        dependencies.add(built.get(dependency));
                    }
                    Descriptors.FileDescriptor[] depsArray = dependencies.toArray(new Descriptors.FileDescriptor[0]);
                    Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, depsArray);
                    built.put(fileName, fileDescriptor);
                    progress = true;
                } catch (Descriptors.DescriptorValidationException ignored) {
                    // Skip invalid descriptors; warnings are emitted by binding resolver.
                }
            }
        }
        if (built.size() < descriptorSet.getFileCount()) {
            List<String> unresolved = new ArrayList<>();
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                String fileName = fileProto.getName();
                if (!built.containsKey(fileName)) {
                    unresolved.add(fileName);
                }
            }
            LOG.warnf("Protobuf descriptor resolution incomplete; unresolved files: %s", unresolved);
        }
        return built;
    }

    /**
     * Resolve the Java class name for a Protobuf message descriptor, honoring file-level
     * `java_package` and `java_multiple_files` options and nested message containment.
     *
     * @param descriptor the Protobuf message descriptor to resolve; may be null
     * @return the resolved ClassName for the message type, or `null` if `descriptor` is null
     */
    private ClassName resolveMessageClassName(Descriptors.Descriptor descriptor) {
        if (descriptor == null) {
            return null;
        }
        Descriptors.FileDescriptor fileDescriptor = descriptor.getFile();
        String javaPkg = fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();

        List<String> nesting = new ArrayList<>();
        Descriptors.Descriptor current = descriptor;
        while (current != null) {
            nesting.add(0, current.getName());
            current = current.getContainingType();
        }

        if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
            String outer = nesting.get(0);
            String[] nested = nesting.size() > 1
                ? nesting.subList(1, nesting.size()).toArray(new String[0])
                : new String[0];
            return ClassName.get(javaPkg, outer, nested);
        }

        String outerClass = deriveOuterClassName(fileDescriptor);
        List<String> full = new ArrayList<>();
        full.add(outerClass);
        full.addAll(nesting);
        String[] nested = full.subList(1, full.size()).toArray(new String[0]);
        return ClassName.get(javaPkg, full.get(0), nested);
    }

    /**
     * Produces the Java outer class name for a protobuf file descriptor.
     *
     * If the file descriptor specifies `java_outer_classname`, that value is returned.
     * Otherwise the protobuf filename (with a trailing `.proto` removed) is converted
     * into a PascalCase identifier by splitting on non-alphanumeric characters and
     * capitalizing each resulting segment.
     *
     * @param fileDescriptor the protobuf file descriptor to derive the outer class name from
     * @return the resolved Java outer class name for the protobuf file
     */
    private String deriveOuterClassName(Descriptors.FileDescriptor fileDescriptor) {
        if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
            return fileDescriptor.getOptions().getJavaOuterClassname();
        }
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
        return sb.toString();
    }

    /**
         * Generate all code artifacts for a single pipeline step model based on its enabled generation targets
         * and the provided bindings and renderers.
         *
         * <p>The method invokes appropriate renderers to produce gRPC services, client steps, REST resources,
         * and REST clients as configured, records generated class roles via the provided RoleMetadataGenerator,
         * and may generate side-effect beans when the model requires them. Generation for a target is skipped
         * when required bindings are absent or when deployment/hosting conditions indicate the target should not
         * be produced.</p>
         *
         * @param ctx the compilation context containing environment, aspect models, and configuration
         * @param model the pipeline step model to generate artifacts for
         * @param grpcBinding gRPC binding information for rendering gRPC-related artifacts; may be null if not available
         * @param restBinding REST binding information for rendering REST-related artifacts; may be null if not available
         * @param descriptorSet protobuf descriptor set used when generating protobuf-related artifacts; may be null
         * @param cacheKeyGenerator optional cache key generator class to include in generation contexts; may be null
         * @param roleMetadataGenerator generator used to record generated classes and their deployment roles
         * @param grpcRenderer renderer responsible for producing gRPC service classes
         * @param clientRenderer renderer responsible for producing gRPC client step classes
         * @param restClientRenderer renderer responsible for producing REST client step classes
         * @param restRenderer renderer responsible for producing REST resource classes
         * @throws IOException if an I/O error occurs while writing generated sources or renderers perform IO
         */
    private void generateArtifacts(
            PipelineCompilationContext ctx,
            PipelineStepModel model,
            GrpcBinding grpcBinding,
            RestBinding restBinding,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            ClassName cacheKeyGenerator,
            RoleMetadataGenerator roleMetadataGenerator,
            GrpcServiceAdapterRenderer grpcRenderer,
            org.pipelineframework.processor.renderer.ClientStepRenderer clientRenderer,
            RestClientStepRenderer restClientRenderer,
            RestResourceRenderer restRenderer) throws IOException {
        
        Set<String> enabledAspects = ctx.getAspectModels().stream()
            .map(aspect -> aspect.name().toLowerCase())
            .collect(java.util.stream.Collectors.toUnmodifiableSet());

        for (GenerationTarget target : model.enabledTargets()) {
            switch (target) {
                case GRPC_SERVICE -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER && !ctx.isPluginHost()) {
                        break;
                    }
                    if (model.sideEffect() && model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER) {
                        generateSideEffectBean(ctx, model, org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER, grpcBinding);
                    }
                    String grpcClassName = model.servicePackage() + ".pipeline." +
                            model.generatedName() + "GrpcService";
                    org.pipelineframework.processor.ir.DeploymentRole grpcRole = model.deploymentRole();
                    grpcRenderer.render(grpcBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, grpcRole),
                        grpcRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(grpcClassName, grpcRole.name());
                }
                case CLIENT_STEP -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
                        break;
                    }
                    String clientClassName = model.servicePackage() + ".pipeline." +
                            model.generatedName().replace("Service", "") + "GrpcClientStep";
                    org.pipelineframework.processor.ir.DeploymentRole clientRole = resolveClientRole(model.deploymentRole());
                    clientRenderer.render(grpcBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, clientRole),
                        clientRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(clientClassName, clientRole.name());
                }
                case REST_RESOURCE -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER && !ctx.isPluginHost()) {
                        break;
                    }
                    if (model.sideEffect() && model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER) {
                        generateSideEffectBean(ctx, model, org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER, grpcBinding);
                    }
                    if (restBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping REST resource generation for '" + model.generatedName() +
                                "' because no REST binding is available.");
                        break;
                    }
                    String restClassName = model.servicePackage() + ".pipeline." +
                            model.generatedName().replace("Service", "").replace("Reactive", "") + "Resource";
                    org.pipelineframework.processor.ir.DeploymentRole restRole = org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER;
                    restRenderer.render(restBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, restRole),
                        restRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(restClassName, restRole.name());
                }
                case REST_CLIENT_STEP -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
                        break;
                    }
                    if (restBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping REST client step generation for '" + model.generatedName() +
                                "' because no REST binding is available.");
                        break;
                    }
                    String restClientClassName = model.servicePackage() + ".pipeline." +
                            model.generatedName().replace("Service", "") + "RestClientStep";
                    org.pipelineframework.processor.ir.DeploymentRole restClientRole = resolveClientRole(model.deploymentRole());
                    restClientRenderer.render(restBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, restClientRole),
                        restClientRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(restClientClassName, restClientRole.name());
                }
            }
        }
    }

    /**
     * Generates a side-effect CDI bean class that extends the step's service type with the resolved observed type
     * and writes it to the role-specific generated sources directory.
     *
     * @param ctx the compilation context used to access the processing environment and output directories
     * @param model the pipeline step model describing the service to extend and naming/packaging metadata
     * @param role the deployment role that determines the generated class's annotation value and output subdirectory
     * @param grpcBinding an optional gRPC binding used to resolve gRPC-specific observed types (may be null)
     */
    private void generateSideEffectBean(
            PipelineCompilationContext ctx,
            PipelineStepModel model,
            org.pipelineframework.processor.ir.DeploymentRole role,
            GrpcBinding grpcBinding) {
        if (model == null || model.serviceClassName() == null) {
            return;
        }

        com.squareup.javapoet.TypeName observedType = resolveObservedType(model, role, grpcBinding);

        com.squareup.javapoet.TypeName parentType = com.squareup.javapoet.ParameterizedTypeName.get(
            model.serviceClassName(), observedType);

        String packageName = model.servicePackage() + org.pipelineframework.processor.PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX;
        String serviceClassName = model.serviceName();

        com.squareup.javapoet.TypeSpec.Builder beanBuilder = com.squareup.javapoet.TypeSpec.classBuilder(serviceClassName)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .superclass(parentType)
            .addAnnotation(com.squareup.javapoet.AnnotationSpec.builder(
                com.squareup.javapoet.ClassName.get("jakarta.enterprise.context", "Dependent"))
                .build())
            .addAnnotation(com.squareup.javapoet.AnnotationSpec.builder(
                com.squareup.javapoet.ClassName.get("io.quarkus.arc", "Unremovable"))
                .build())
            .addAnnotation(com.squareup.javapoet.AnnotationSpec.builder(
                com.squareup.javapoet.ClassName.get("org.pipelineframework.annotation", "GeneratedRole"))
                .addMember("value", "$T.$L",
                    com.squareup.javapoet.ClassName.get("org.pipelineframework.annotation.GeneratedRole", "Role"),
                    role.name())
                .build());

        javax.lang.model.element.TypeElement pluginElement =
            ctx.getProcessingEnv().getElementUtils().getTypeElement(model.serviceClassName().canonicalName());
        com.squareup.javapoet.MethodSpec constructor = buildSideEffectConstructor(ctx, pluginElement);
        if (constructor != null) {
            beanBuilder.addMethod(constructor);
        }

        com.squareup.javapoet.TypeSpec beanClass = beanBuilder.build();

        try {
            com.squareup.javapoet.JavaFile.builder(packageName, beanClass)
                .build()
                .writeTo(resolveRoleOutputDir(ctx, role));
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to generate side-effect bean for '" + model.serviceName() + "': " + e.getMessage());
            }
        }
    }

    private com.squareup.javapoet.MethodSpec buildSideEffectConstructor(
            PipelineCompilationContext ctx,
            javax.lang.model.element.TypeElement pluginElement) {
        if (pluginElement == null) {
            return null;
        }

        java.util.List<javax.lang.model.element.ExecutableElement> constructors = pluginElement.getEnclosedElements().stream()
            .filter(element -> element.getKind() == javax.lang.model.element.ElementKind.CONSTRUCTOR)
            .map(element -> (javax.lang.model.element.ExecutableElement) element)
            .toList();

        if (constructors.isEmpty()) {
            return null;
        }

        javax.lang.model.element.ExecutableElement selected = selectConstructor(constructors);
        java.util.List<? extends javax.lang.model.element.VariableElement> params = selected.getParameters();
        if (params.isEmpty()) {
            return com.squareup.javapoet.MethodSpec.constructorBuilder()
                .addAnnotation(com.squareup.javapoet.ClassName.get("jakarta.inject", "Inject"))
                .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
                .addStatement("super()")
                .build();
        }

        com.squareup.javapoet.MethodSpec.Builder builder = com.squareup.javapoet.MethodSpec.constructorBuilder()
            .addAnnotation(com.squareup.javapoet.ClassName.get("jakarta.inject", "Inject"))
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC);

        java.util.List<String> argNames = new java.util.ArrayList<>();
        int index = 0;
        for (javax.lang.model.element.VariableElement param : params) {
            String name = param.getSimpleName().toString();
            if (name == null || name.isBlank()) {
                name = "arg" + index;
            }
            argNames.add(name);
            builder.addParameter(com.squareup.javapoet.TypeName.get(param.asType()), name);
            index++;
        }

        builder.addStatement("super($L)", String.join(", ", argNames));
        return builder.build();
    }

    /**
     * Selects the preferred constructor from a list of constructors using a deterministic priority.
     *
     * The selection priority is: a constructor annotated with `@Inject`; if none, the sole constructor
     * when the list has size one; if none, a no-argument constructor; otherwise the first constructor
     * in the list.
     *
     * @param constructors the available constructors to select from
     * @return the chosen `ExecutableElement` constructor according to the priority rules
     */
    private javax.lang.model.element.ExecutableElement selectConstructor(
            java.util.List<javax.lang.model.element.ExecutableElement> constructors) {
        for (javax.lang.model.element.ExecutableElement constructor : constructors) {
            if (constructor.getAnnotation(jakarta.inject.Inject.class) != null) {
                return constructor;
            }
        }
        if (constructors.size() == 1) {
            return constructors.get(0);
        }
        for (javax.lang.model.element.ExecutableElement constructor : constructors) {
            if (constructor.getParameters().isEmpty()) {
                return constructor;
            }
        }
        return constructors.get(0);
    }

    /**
     * Resolve the effective observed type for a pipeline step, applying cache-plugin and transport-specific conversions.
     *
     * The method selects the step's outbound domain type if present, otherwise the inbound domain type, and uses
     * java.lang.Object if neither is available. If the step is not the cache plugin the selected domain type is returned
     * unchanged. For REST server deployments the domain type is converted to a corresponding DTO type. For plugin server
     * deployments with a provided gRPC binding, an attempt is made to map the domain type to the gRPC parameter or return
     * type via GrpcJavaTypeResolver; if a message or service name matches the domain type the corresponding gRPC type is
     * returned, otherwise the resolver's return type is used. If gRPC type resolution fails, the original domain type is
     * returned.
     *
     * @param model the pipeline step model to resolve the observed type for
     * @param role the deployment role where the step will run
     * @param grpcBinding optional gRPC binding used to resolve gRPC parameter/return types when applicable
     * @return the resolved TypeName to be observed by the step (or `Object` if no domain type is available)
     */
    private com.squareup.javapoet.TypeName resolveObservedType(
            PipelineStepModel model,
            org.pipelineframework.processor.ir.DeploymentRole role,
            GrpcBinding grpcBinding) {
        com.squareup.javapoet.TypeName observedType = model.outboundDomainType() != null
            ? model.outboundDomainType()
            : model.inboundDomainType();
        if (observedType == null) {
            return com.squareup.javapoet.ClassName.OBJECT;
        }
        if (!isCachePlugin(model)) {
            return observedType;
        }
        if (role == org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER) {
            return convertDomainToDtoType(observedType);
        }
        if (role == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER && grpcBinding != null) {
            try {
                org.pipelineframework.processor.util.GrpcJavaTypeResolver resolver =
                    new org.pipelineframework.processor.util.GrpcJavaTypeResolver();
                var grpcTypes = resolver.resolve(grpcBinding);
                if (grpcTypes != null && grpcTypes.grpcParameterType() != null && grpcTypes.grpcReturnType() != null) {
                    Object methodDescriptorObj = grpcBinding.methodDescriptor();
                    if (methodDescriptorObj instanceof com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor) {
                        String inputFullName = methodDescriptor.getInputType().getFullName();
                        String outputFullName = methodDescriptor.getOutputType().getFullName();
                        String inputName = methodDescriptor.getInputType().getName();
                        String outputName = methodDescriptor.getOutputType().getName();
                        String observedTypeName = observedType != null ? observedType.toString() : null;

                        if (observedTypeName != null) {
                            if (observedTypeName.equals(inputFullName) || observedTypeName.endsWith("." + inputName)) {
                                return grpcTypes.grpcParameterType();
                            }
                            if (observedTypeName.equals(outputFullName) || observedTypeName.endsWith("." + outputName)) {
                                return grpcTypes.grpcReturnType();
                            }
                        }

                        String serviceName = model.serviceName();
                        if (serviceName != null) {
                            if (serviceName.equals(inputFullName) || serviceName.equals(inputName)) {
                                return grpcTypes.grpcParameterType();
                            }
                            if (serviceName.equals(outputFullName) || serviceName.equals(outputName)) {
                                return grpcTypes.grpcReturnType();
                            }
                        }
                    }
                    return grpcTypes.grpcReturnType();
                }
            } catch (ClassCastException | NullPointerException | IllegalStateException e) {
                LOG.warnf(e, "Failed to resolve observed gRPC type for %s; falling back to domain type",
                    model.serviceName());
                return observedType;
            }
        }
        return observedType;
    }

    /**
     * Determines whether the given pipeline step model represents the cache plugin.
     *
     * @param model the pipeline step model to inspect
     * @return `true` if the model's service class is `org.pipelineframework.plugin.cache.CacheService`, `false` otherwise
     */
    private boolean isCachePlugin(PipelineStepModel model) {
        if (model == null || model.serviceClassName() == null) {
            return false;
        }
        String name = model.serviceClassName().canonicalName();
        return "org.pipelineframework.plugin.cache.CacheService".equals(name);
    }

    /**
     * Derives the DTO type corresponding to a given domain type.
     *
     * @param domainType the domain TypeName to convert; may be null
     * @return the TypeName for the corresponding DTO class (appends `Dto` when needed); returns `Object` when `domainType` is null
     */
    private com.squareup.javapoet.TypeName convertDomainToDtoType(com.squareup.javapoet.TypeName domainType) {
        if (domainType == null) {
            return com.squareup.javapoet.ClassName.OBJECT;
        }
        String domainTypeStr = domainType.toString();
        String dtoTypeStr = domainTypeStr
            .replace(".domain.", ".dto.")
            .replace(".service.", ".dto.");
        if (!dtoTypeStr.equals(domainTypeStr)) {
            int lastDot = dtoTypeStr.lastIndexOf('.');
            String packageName = lastDot > 0 ? dtoTypeStr.substring(0, lastDot) : "";
            String simpleName = lastDot > 0 ? dtoTypeStr.substring(lastDot + 1) : dtoTypeStr;
            String dtoSimpleName = simpleName.endsWith("Dto") ? simpleName : simpleName + "Dto";
            return com.squareup.javapoet.ClassName.get(packageName, dtoSimpleName);
        }
        int lastDot = domainTypeStr.lastIndexOf('.');
        String packageName = lastDot > 0 ? domainTypeStr.substring(0, lastDot) : "";
        String simpleName = lastDot > 0 ? domainTypeStr.substring(lastDot + 1) : domainTypeStr;
        String dtoSimpleName = simpleName.endsWith("Dto") ? simpleName : simpleName + "Dto";
        return com.squareup.javapoet.ClassName.get(packageName, dtoSimpleName);
    }

    /**
     * Generates orchestrator server artifacts.
     * 
     * @param ctx the compilation context
     * @param descriptorSet the protobuf descriptor set
     * @param generateCli whether to generate CLI
     * @param orchestratorGrpcRenderer gRPC orchestrator renderer
     * @param orchestratorRestRenderer REST orchestrator renderer
     * @param orchestratorCliRenderer CLI orchestrator renderer
     * @param roleMetadataGenerator role metadata generator
     * @param cacheKeyGenerator cache key generator
     */
    private void generateOrchestratorServer(
            PipelineCompilationContext ctx,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            boolean generateCli,
            OrchestratorGrpcRenderer orchestratorGrpcRenderer,
            OrchestratorRestResourceRenderer orchestratorRestRenderer,
            OrchestratorCliRenderer orchestratorCliRenderer,
            RoleMetadataGenerator roleMetadataGenerator,
            ClassName cacheKeyGenerator) {
        // Get orchestrator binding from context
        Object bindingObj = ctx.getRendererBindings().get("orchestrator");
        if (!(bindingObj instanceof org.pipelineframework.processor.ir.OrchestratorBinding binding)) {
            return;
        }

        try {
            String transport = binding.normalizedTransport();
            if ("REST".equalsIgnoreCase(transport)) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER;
                orchestratorRestRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
            } else {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER;
                orchestratorGrpcRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
            }

            if (generateCli) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
                orchestratorCliRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
            }
        } catch (IOException e) {
            ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR,
                "Failed to generate orchestrator server: " + e.getMessage());
        }
    }

    /**
     * Resolves the cache key generator from processing environment options.
     * 
     * @param ctx the compilation context
     * @return the cache key generator class name or null
     */
    private ClassName resolveCacheKeyGenerator(PipelineCompilationContext ctx) {
        String configured = ctx.getProcessingEnv().getOptions().get("pipeline.cache.keyGenerator");
        if (configured == null || configured.isBlank()) {
            return null;
        }
        return ClassName.bestGuess(configured);
    }

    /**
     * Resolves the client role based on the server role.
     *
     * @param serverRole the original server role
     * @return the corresponding client role
     */
    private org.pipelineframework.processor.ir.DeploymentRole resolveClientRole(
            org.pipelineframework.processor.ir.DeploymentRole serverRole) {
        return switch (serverRole) {
            case PLUGIN_SERVER -> org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_CLIENT;
            case PIPELINE_SERVER -> org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
            case ORCHESTRATOR_CLIENT, PLUGIN_CLIENT, REST_SERVER -> serverRole;
        };
    }

    private java.nio.file.Path resolveRoleOutputDir(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.DeploymentRole role) {
        java.nio.file.Path root = ctx.getGeneratedSourcesRoot();
        if (root == null || role == null) {
            return root;
        }
        String dirName = role.name().toLowerCase().replace('_', '-');
        java.nio.file.Path outputDir = root.resolve(dirName);
        try {
            java.nio.file.Files.createDirectories(outputDir);
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to create output directory '" + outputDir + "': " + e.getMessage());
            }
        }
        return outputDir;
    }
}
