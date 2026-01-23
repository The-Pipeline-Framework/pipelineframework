package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.renderer.*;
import org.pipelineframework.processor.util.OrchestratorClientPropertiesGenerator;
import org.pipelineframework.processor.util.PipelineOrderMetadataGenerator;
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
                        generateSideEffectBean(ctx, model, org.pipelineframework.processor.ir.DeploymentRole.PLUGIN_SERVER);
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
                        generateSideEffectBean(ctx, model, org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER);
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
            org.pipelineframework.processor.ir.DeploymentRole role) {
        if (model == null || model.serviceClassName() == null) {
            return;
        }

        com.squareup.javapoet.TypeName observedType = model.outboundDomainType() != null
            ? model.outboundDomainType()
            : model.inboundDomainType();
        if (observedType == null) {
            observedType = com.squareup.javapoet.ClassName.OBJECT;
        }

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
