package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.*;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
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

    /**
     * Creates a new PipelineGenerationPhase.
     */
    public PipelineGenerationPhase() {
    }

    @Override
    public String name() {
        return "Pipeline Generation Phase";
    }

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
                String parserName = "Proto" + String.join("", messageType.simpleNames()) + "Parser";
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

    private Map<String, Descriptors.FileDescriptor> buildFileDescriptors(
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
        if (descriptorSet == null) {
            return built;
        }
        int iterations = 0;
        boolean progress = true;
        while (built.size() < descriptorSet.getFileCount() && progress) {
            progress = false;
            iterations++;
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
        return built;
    }

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
     * Generate artifacts for the given pipeline step model using the provided bindings and renderers.
     * 
     * @param ctx the compilation context
     * @param model the pipeline step model
     * @param grpcBinding gRPC binding information
     * @param restBinding REST binding information
     * @param descriptorSet protobuf descriptor set
     * @param cacheKeyGenerator cache key generator class
     * @param roleMetadataGenerator role metadata generator
     * @param grpcRenderer gRPC service renderer
     * @param clientRenderer client step renderer
     * @param restClientRenderer REST client step renderer
     * @param restRenderer REST resource renderer
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
     * Generates a side effect bean for the given model.
     *
     * @param ctx the compilation context
     * @param model the pipeline step model
     * @param role the deployment role
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
                    String inputName = null;
                    String outputName = null;
                    Object methodDescriptorObj = grpcBinding.methodDescriptor();
                    if (methodDescriptorObj instanceof com.google.protobuf.Descriptors.MethodDescriptor methodDescriptor) {
                        inputName = methodDescriptor.getInputType().getName();
                        outputName = methodDescriptor.getOutputType().getName();
                    }
                    String serviceName = model.serviceName();
                    if (inputName != null && serviceName != null && serviceName.contains(inputName)) {
                        return grpcTypes.grpcParameterType();
                    }
                    if (outputName != null && serviceName != null && serviceName.contains(outputName)) {
                        return grpcTypes.grpcReturnType();
                    }
                    return grpcTypes.grpcReturnType();
                }
            } catch (Exception ignored) {
                return observedType;
            }
        }
        return observedType;
    }

    private boolean isCachePlugin(PipelineStepModel model) {
        if (model == null || model.serviceClassName() == null) {
            return false;
        }
        String name = model.serviceClassName().canonicalName();
        return "org.pipelineframework.plugin.cache.CacheService".equals(name);
    }

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
            String dtoSimpleName = simpleName + "Dto";
            return com.squareup.javapoet.ClassName.get(packageName, dtoSimpleName);
        }
        int lastDot = domainTypeStr.lastIndexOf('.');
        String packageName = lastDot > 0 ? domainTypeStr.substring(0, lastDot) : "";
        String simpleName = lastDot > 0 ? domainTypeStr.substring(lastDot + 1) : domainTypeStr;
        String dtoSimpleName = simpleName + "Dto";
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
