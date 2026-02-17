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
import org.pipelineframework.processor.StepDefinitionWriter;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.renderer.*;
import org.pipelineframework.processor.util.OrchestratorClientPropertiesGenerator;
import org.pipelineframework.processor.util.PipelineOrderMetadataGenerator;
import org.pipelineframework.processor.util.PipelinePlatformMetadataGenerator;
import org.pipelineframework.processor.util.PipelineTelemetryMetadataGenerator;
import org.pipelineframework.processor.util.ResourceNameUtils;
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
    private static final String ORCHESTRATOR_BINDING_KEY = "orchestrator";
    private static final String SERVICE_SUFFIX = "Service";

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
         * Generates pipeline source artifacts and metadata for all configured step models and the orchestrator.
         *
         * <p>Produces per-step artifacts (gRPC services, REST resources, client classes, side-effect beans), optionally
         * generates protobuf parsers when applicable, and writes role, platform, and orchestrator-related metadata.</p>
         *
         * @param ctx the compilation context containing step models, renderer bindings, descriptor set, environment, and settings
         * @throws Exception if an unrecoverable generation error occurs that cannot be handled locally
         */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Only proceed if there are models to process or orchestrator to generate
        if (ctx.getStepModels().isEmpty() && !ctx.isOrchestratorGenerated()) {
            // Still need to write role metadata even if no models to process
            RoleMetadataGenerator roleMetadataGenerator = new RoleMetadataGenerator(ctx.getProcessingEnv());
            PipelinePlatformMetadataGenerator platformMetadataGenerator =
                new PipelinePlatformMetadataGenerator(ctx.getProcessingEnv());
            try {
                roleMetadataGenerator.writeRoleMetadata();
            } catch (IOException e) {
                if (ctx.getProcessingEnv() != null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.WARNING,
                        "Failed to write role metadata: " + e.getMessage());
                }
            }
            try {
                platformMetadataGenerator.writePlatformMetadata(ctx);
            } catch (IOException e) {
                if (ctx.getProcessingEnv() != null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.WARNING,
                        "Failed to write platform metadata: " + e.getMessage());
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
        org.pipelineframework.processor.renderer.LocalClientStepRenderer localClientRenderer =
            new org.pipelineframework.processor.renderer.LocalClientStepRenderer();
        RestClientStepRenderer restClientRenderer = new RestClientStepRenderer();
        RestResourceRenderer restRenderer = new RestResourceRenderer();
        RestFunctionHandlerRenderer restFunctionHandlerRenderer = new RestFunctionHandlerRenderer();
        OrchestratorGrpcRenderer orchestratorGrpcRenderer = new OrchestratorGrpcRenderer();
        OrchestratorRestResourceRenderer orchestratorRestRenderer = new OrchestratorRestResourceRenderer();
        OrchestratorFunctionHandlerRenderer orchestratorFunctionHandlerRenderer =
            new OrchestratorFunctionHandlerRenderer();
        OrchestratorCliRenderer orchestratorCliRenderer = new OrchestratorCliRenderer();
        OrchestratorIngestClientRenderer orchestratorIngestClientRenderer = new OrchestratorIngestClientRenderer();
        ExternalAdapterRenderer externalAdapterRenderer = new ExternalAdapterRenderer(GenerationTarget.EXTERNAL_ADAPTER);

        // Initialize role metadata generator
        RoleMetadataGenerator roleMetadataGenerator = new RoleMetadataGenerator(ctx.getProcessingEnv());
        PipelinePlatformMetadataGenerator platformMetadataGenerator =
            new PipelinePlatformMetadataGenerator(ctx.getProcessingEnv());

        // Get the cache key generator
        ClassName cacheKeyGenerator = resolveCacheKeyGenerator(ctx);

        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();

        Set<String> generatedSideEffectBeans = new HashSet<>();
        Set<String> enabledAspects = computeEnabledAspects(ctx);

        // Generate artifacts for each step model
        for (PipelineStepModel model : ctx.getStepModels()) {
            // Check if this is a delegation step
            Object externalAdapterBindingObj = bindingsMap.get(model.serviceName() + "_external_adapter");
            ExternalAdapterBinding externalAdapterBinding = null;
            if (externalAdapterBindingObj instanceof ExternalAdapterBinding) {
                externalAdapterBinding = (ExternalAdapterBinding) externalAdapterBindingObj;
            } else if (externalAdapterBindingObj != null) {
                LOG.warnf("Invalid binding type for '%s_external_adapter': expected ExternalAdapterBinding but got %s",
                    model.serviceName(), externalAdapterBindingObj.getClass().getName());
            }
            
            if (externalAdapterBinding != null) {
                // For delegation steps, generate the external adapter instead of regular artifacts
                // Use the deployment role from the model to determine output directory
                org.pipelineframework.processor.ir.DeploymentRole adapterRole = model.deploymentRole() != null
                    ? model.deploymentRole()
                    : org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER;
                try {
                    externalAdapterRenderer.render(externalAdapterBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, adapterRole),
                        adapterRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    String generatedName = model.generatedName() != null ? model.generatedName() : model.serviceName();
                    String baseName = generatedName.endsWith("Service")
                        ? generatedName.substring(0, generatedName.length() - "Service".length())
                        : generatedName;
                    String externalAdapterClassName = model.servicePackage() + ".pipeline." + baseName + "ExternalAdapter";
                    roleMetadataGenerator.recordClassWithRole(externalAdapterClassName, adapterRole.name());
                } catch (IOException e) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.ERROR,
                        "Failed to generate external adapter for '" + model.serviceName() + "': " + e.getMessage());
                }
            }

            // Get the bindings for this model (for non-delegation steps)
            GrpcBinding grpcBinding = (GrpcBinding) bindingsMap.get(model.serviceName() + "_grpc");
            RestBinding restBinding = (RestBinding) bindingsMap.get(model.serviceName() + "_rest");
            LocalBinding localBinding = (LocalBinding) bindingsMap.get(model.serviceName() + "_local");

            // Generate artifacts based on enabled targets
            generateArtifacts(
                ctx,
                model,
                grpcBinding,
                restBinding,
                localBinding,
                generatedSideEffectBeans,
                enabledAspects,
                descriptorSet,
                cacheKeyGenerator,
                roleMetadataGenerator,
                grpcRenderer,
                clientRenderer,
                localClientRenderer,
                restClientRenderer,
                restRenderer,
                restFunctionHandlerRenderer
            );
        }

        if (ctx.isTransportModeGrpc() && descriptorSet != null) {
            generateProtobufParsers(ctx, descriptorSet);
        }

        // Generate orchestrator artifacts if needed
        if (ctx.isOrchestratorGenerated()) {
            OrchestratorBinding orchestratorBinding = (OrchestratorBinding) bindingsMap.get(ORCHESTRATOR_BINDING_KEY);
            if (orchestratorBinding != null) {
                generateOrchestratorServer(
                    ctx,
                    descriptorSet,
                    orchestratorBinding.cliName() != null, // Using cliName as indicator for CLI generation
                    orchestratorGrpcRenderer,
                    orchestratorRestRenderer,
                    orchestratorFunctionHandlerRenderer,
                    orchestratorCliRenderer,
                    orchestratorIngestClientRenderer,
                    roleMetadataGenerator,
                    cacheKeyGenerator
                );
            }
        }

        // Write role metadata
        try {
            roleMetadataGenerator.writeRoleMetadata();
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to write role metadata: " + e.getMessage());
            }
        }
        try {
            platformMetadataGenerator.writePlatformMetadata(ctx);
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to write platform metadata: " + e.getMessage());
            }
        }
        try {
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
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to write orchestrator metadata: " + e.getMessage());
            }
        }

        // Write step definitions for Quarkus build step consumption
        if (ctx.getProcessingEnv() == null) {
            LOG.warn("Skipping step definition writing because ProcessingEnvironment is null");
            return;
        }
        try {
            StepDefinitionWriter stepDefinitionWriter = new StepDefinitionWriter(ctx.getProcessingEnv().getFiler());
            stepDefinitionWriter.write(ctx.getStepModels());
        } catch (IOException e) {
            ctx.getProcessingEnv().getMessager().printMessage(
                javax.tools.Diagnostic.Kind.WARNING,
                "Failed to write step definitions: " + e.getMessage());
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
        int slashIndex = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
        if (slashIndex >= 0 && slashIndex + 1 < fileName.length()) {
            fileName = fileName.substring(slashIndex + 1);
        }
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

    private String trimServiceSuffix(String generatedName) {
        if (generatedName != null && generatedName.endsWith(SERVICE_SUFFIX)) {
            return generatedName.substring(0, generatedName.length() - SERVICE_SUFFIX.length());
        }
        return generatedName;
    }

    /**
     * Generate all source and metadata artifacts for a single pipeline step according to its enabled generation targets.
     *
     * This will invoke the appropriate renderers to produce gRPC services, client steps (gRPC, local, REST),
     * REST resources, record generated classes with their deployment roles, and generate side-effect CDI beans
     * when required and permitted by the compilation context.
     *
     * @param ctx the compilation context containing processing environment, aspect models, transport and hosting configuration
     * @param model the pipeline step model to generate artifacts for
     * @param grpcBinding gRPC binding information for rendering gRPC-related artifacts; may be null
     * @param restBinding REST binding information for rendering REST-related artifacts; may be null
     * @param localBinding local-transport binding information for rendering local client artifacts; may be null
     * @param generatedSideEffectBeans a set used to track already-generated side-effect bean keys to avoid duplicates
     * @param descriptorSet protobuf descriptor set used for protobuf-related generation; may be null
     * @param cacheKeyGenerator optional cache key generator class to include in generation contexts; may be null
     * @param roleMetadataGenerator generator used to record generated class names and their deployment roles
     * @param grpcRenderer renderer responsible for producing gRPC service classes
     * @param clientRenderer renderer responsible for producing gRPC client step classes
     * @param localClientRenderer renderer responsible for producing local-transport client step classes
     * @param restClientRenderer renderer responsible for producing REST client step classes
     * @param restRenderer renderer responsible for producing REST resource classes
     * @param restFunctionHandlerRenderer renderer responsible for producing native function handlers for unary REST resources
     * @throws IOException if an I/O error occurs while writing generated sources or renderers perform IO
     */
    private void generateArtifacts(
            PipelineCompilationContext ctx,
            PipelineStepModel model,
            GrpcBinding grpcBinding,
            RestBinding restBinding,
            LocalBinding localBinding,
            Set<String> generatedSideEffectBeans,
            Set<String> enabledAspects,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            ClassName cacheKeyGenerator,
            RoleMetadataGenerator roleMetadataGenerator,
            GrpcServiceAdapterRenderer grpcRenderer,
            org.pipelineframework.processor.renderer.ClientStepRenderer clientRenderer,
            org.pipelineframework.processor.renderer.LocalClientStepRenderer localClientRenderer,
            RestClientStepRenderer restClientRenderer,
            RestResourceRenderer restRenderer,
            RestFunctionHandlerRenderer restFunctionHandlerRenderer) throws IOException {
        
        for (GenerationTarget target : model.enabledTargets()) {
            switch (target) {
                case GRPC_SERVICE -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER
                        && !allowPluginServerArtifacts(ctx)) {
                        break;
                    }
                    if (model.sideEffect() && model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER) {
                        org.pipelineframework.processor.ir.DeploymentRole sideEffectOutputRole = ctx.isTransportModeLocal()
                            ? org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT
                            : org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER;
                        if (ctx.isTransportModeLocal()) {
                            String sideEffectBeanKey = model.servicePackage() + ".pipeline." + model.serviceName();
                            if (generatedSideEffectBeans.add(sideEffectBeanKey)) {
                                generateSideEffectBean(
                                    ctx,
                                    model,
                                    org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER,
                                    sideEffectOutputRole,
                                    grpcBinding);
                            }
                        } else {
                            generateSideEffectBean(
                                ctx,
                                model,
                                org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER,
                                sideEffectOutputRole,
                                grpcBinding);
                        }
                    }
                    if (ctx.isTransportModeLocal()) {
                        break;
                    }
                    if (grpcBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping gRPC service generation for '" + model.generatedName() +
                                "' because no gRPC binding is available.");
                        break;
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
                    if (grpcBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping gRPC client step generation for '" + model.generatedName() +
                                "' because no gRPC binding is available.");
                        break;
                    }
                    String clientClassName = model.servicePackage() + ".pipeline."
                        + trimServiceSuffix(model.generatedName()) + "GrpcClientStep";
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
                case LOCAL_CLIENT_STEP -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
                        break;
                    }
                    if (ctx.getProcessingEnv() == null) {
                        break;
                    }
                    if (model.sideEffect()) {
                        String sideEffectBeanKey = model.servicePackage() + ".pipeline." + model.serviceName();
                        if (generatedSideEffectBeans.add(sideEffectBeanKey)) {
                            org.pipelineframework.processor.ir.DeploymentRole sideEffectRole =
                                model.deploymentRole() == null
                                    ? org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT
                                    : model.deploymentRole();
                            generateSideEffectBean(
                                ctx,
                                model,
                                sideEffectRole,
                                org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT,
                                grpcBinding);
                        }
                    }
                    if (localBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping local client step generation for '" + model.generatedName() +
                                "' because no local binding is available.");
                        break;
                    }
                    String localClientClassName = model.servicePackage() + ".pipeline."
                        + trimServiceSuffix(model.generatedName()) + "LocalClientStep";
                    org.pipelineframework.processor.ir.DeploymentRole localClientRole = resolveClientRole(model.deploymentRole());
                    localClientRenderer.render(localBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, localClientRole),
                        localClientRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(localClientClassName, localClientRole.name());
                }
                case REST_RESOURCE -> {
                    if (model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER
                        && !allowPluginServerArtifacts(ctx)) {
                        break;
                    }
                    if (model.sideEffect() && model.deploymentRole() == org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER) {
                        generateSideEffectBean(
                            ctx,
                            model,
                            org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER,
                            org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER,
                            grpcBinding);
                    }
                    if (restBinding == null) {
                        ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                            "Skipping REST resource generation for '" + model.generatedName() +
                                "' because no REST binding is available.");
                        break;
                    }
                    String restClassName = model.servicePackage() + ".pipeline."
                        + ResourceNameUtils.normalizeBaseName(model.generatedName()) + "Resource";
                    org.pipelineframework.processor.ir.DeploymentRole restRole = org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER;
                    restRenderer.render(restBinding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, restRole),
                        restRole,
                        enabledAspects,
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(restClassName, restRole.name());

                    if (ctx.isPlatformModeFunction()) {
                        String handlerClassName =
                            RestFunctionHandlerRenderer.handlerFqcn(model.servicePackage(), model.generatedName());
                        restFunctionHandlerRenderer.render(restBinding, new GenerationContext(
                            ctx.getProcessingEnv(),
                            resolveRoleOutputDir(ctx, restRole),
                            restRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                        roleMetadataGenerator.recordClassWithRole(handlerClassName, restRole.name());
                    }
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
                    String restClientClassName = model.servicePackage() + ".pipeline."
                        + trimServiceSuffix(model.generatedName()) + "RestClientStep";
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

    private Set<String> computeEnabledAspects(PipelineCompilationContext ctx) {
        return Optional.ofNullable(ctx.getAspectModels())
            .orElse(List.of())
            .stream()
            .map(aspect -> aspect.name().toLowerCase(Locale.ROOT))
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    /**
         * Generates a CDI side-effect bean subclass of the step's service type and writes it to the generated-sources
         * directory for the specified role.
         *
         * <p>The generated class is public, Dependent-scoped, marked Unremovable, and annotated with
         * {@code @GeneratedRole} using the provided {@code role}. The bean's generic type parameter is the resolved
         * observed type (which may be derived from gRPC bindings when applicable). If {@code outputRole} is non-null it
         * is used to determine the output subdirectory; otherwise {@code role} is used.</p>
         *
         * <p>If {@code model} is null or has no service class name this method is a no-op. IO failures while writing the
         * generated source are reported as a warning via the processing environment's Messager and not rethrown.</p>
         *
         * @param ctx the compilation context providing the processing environment and output directories
         * @param model the pipeline step model describing the service to extend and naming/packaging metadata
         * @param role the deployment role used for the {@code @GeneratedRole} annotation and default output location
         * @param outputRole optional override for the output role (if null the {@code role} parameter is used)
         * @param grpcBinding optional gRPC binding used to resolve gRPC-specific observed types (may be null)
         */
    private void generateSideEffectBean(
            PipelineCompilationContext ctx,
            PipelineStepModel model,
            org.pipelineframework.processor.ir.DeploymentRole role,
            org.pipelineframework.processor.ir.DeploymentRole outputRole,
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
                    com.squareup.javapoet.ClassName.get("org.pipelineframework.annotation", "GeneratedRole", "Role"),
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
                .writeTo(resolveRoleOutputDir(ctx, outputRole == null ? role : outputRole));
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
         * Resolve the observed domain type for a pipeline step, applying cache-plugin and transport-specific conversions.
         *
         * Selects the step's outbound domain type if present, otherwise the inbound domain type, and uses `Object` if neither
         * is available. If the step is not the cache plugin the selected domain type is returned unchanged. For
         * REST_SERVER deployments the domain type is converted to a corresponding DTO type. For PLUGIN_SERVER deployments
         * with a provided gRPC binding, an attempt is made to map the domain type to the gRPC parameter or return type;
         * resolution failures fall back to the original domain type.
         *
         * @param model the pipeline step model to resolve the observed type for
         * @param role the deployment role where the step will run
         * @param grpcBinding optional gRPC binding used to resolve gRPC parameter/return types when applicable
         * @return the resolved TypeName to be observed by the step; `Object` if no domain type is available
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
         * Determine whether plugin-server artifacts should be generated for the given compilation context.
         *
         * <p>Generation is enabled when the context represents an explicit plugin host, or when a runtime
         * mapping exists and the module name is non-blank (indicating module-aware builds where plugin-server
         * artifacts may coexist). A {@code null} context disables generation.</p>
         *
         * @param ctx compilation context used to evaluate plugin-host status, runtime mapping, and module name
         * @return {@code true} if plugin-server artifacts should be generated, {@code false} otherwise
         */
    private boolean allowPluginServerArtifacts(PipelineCompilationContext ctx) {
        if (ctx == null) {
            return false;
        }
        if (ctx.isPluginHost()) {
            return true;
        }
        String moduleName = ctx.getModuleName();
        return ctx.getRuntimeMapping() != null && moduleName != null && !moduleName.isBlank();
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
     * Generate orchestrator server and client source artifacts based on the orchestrator binding's transport.
     *
     * Renders a REST orchestrator server when transport is REST, a gRPC pipeline server when transport is gRPC (or unspecified), and skips server generation for LOCAL transport; additionally renders a CLI client when {@code generateCli} is true and renders an ingest client when transport is neither REST nor LOCAL. Errors during generation are reported to the processing environment Messager.
     *
     * @param ctx the pipeline compilation context
     * @param descriptorSet protobuf descriptor set used during generation; may be {@code null}
     * @param generateCli whether to generate the CLI orchestrator client
     * @param orchestratorGrpcRenderer renderer for gRPC orchestrator server artifacts
     * @param orchestratorRestRenderer renderer for REST orchestrator server artifacts
     * @param orchestratorFunctionHandlerRenderer renderer for native function orchestrator handlers
     * @param orchestratorCliRenderer renderer for CLI orchestrator client artifacts
     * @param orchestratorIngestClientRenderer renderer for orchestrator ingest client artifacts
     * @param roleMetadataGenerator generator that records generated classes by deployment role
     * @param cacheKeyGenerator optional cache key generator class; may be {@code null}
     */
    private void generateOrchestratorServer(
            PipelineCompilationContext ctx,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            boolean generateCli,
            OrchestratorGrpcRenderer orchestratorGrpcRenderer,
            OrchestratorRestResourceRenderer orchestratorRestRenderer,
            OrchestratorFunctionHandlerRenderer orchestratorFunctionHandlerRenderer,
            OrchestratorCliRenderer orchestratorCliRenderer,
            OrchestratorIngestClientRenderer orchestratorIngestClientRenderer,
            RoleMetadataGenerator roleMetadataGenerator,
            ClassName cacheKeyGenerator) {
        // Get orchestrator binding from context
        Object bindingObj = ctx.getRendererBindings().get("orchestrator");
        if (!(bindingObj instanceof org.pipelineframework.processor.ir.OrchestratorBinding binding)) {
            return;
        }

        try {
            String transport = binding.normalizedTransport();
            boolean rest = "REST".equalsIgnoreCase(transport);
            boolean local = "LOCAL".equalsIgnoreCase(transport);
            if (rest) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER;
                orchestratorRestRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
                if (ctx.isPlatformModeFunction() && !binding.inputStreaming() && !binding.outputStreaming()) {
                    orchestratorFunctionHandlerRenderer.render(binding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, role),
                        role,
                        java.util.Set.of(),
                        cacheKeyGenerator,
                        descriptorSet));
                    roleMetadataGenerator.recordClassWithRole(
                        OrchestratorFunctionHandlerRenderer.handlerFqcn(binding.basePackage()),
                        role.name());
                }
            } else if (!local) {
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

            if (!rest && !local) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
                orchestratorIngestClientRenderer.render(binding, new GenerationContext(
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
        if (serverRole == null) {
            return org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
        }
        return switch (serverRole) {
            case PLUGIN_SERVER -> org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_CLIENT;
            case PIPELINE_SERVER -> org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
            case ORCHESTRATOR_CLIENT, PLUGIN_CLIENT, REST_SERVER -> serverRole;
        };
    }

    /**
     * Resolve and ensure existence of the output directory for the given deployment role under the generated sources root.
     *
     * @param ctx  the compilation context used to obtain the generated sources root and the processing environment for reporting
     * @param role the deployment role whose directory name will be derived (role name lowercased with underscores replaced by dashes)
     * @return the role-specific output Path under the generated sources root; if the generated sources root is null returns null; if role is null returns the generated sources root
     */
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
