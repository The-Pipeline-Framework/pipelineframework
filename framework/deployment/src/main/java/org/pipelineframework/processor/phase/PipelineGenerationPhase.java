package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.jboss.logging.Logger;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.StepDefinitionWriter;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.renderer.*;
import org.pipelineframework.processor.util.OrchestratorClientPropertiesGenerator;
import org.pipelineframework.processor.util.CheckpointHandoffMetadataGenerator;
import org.pipelineframework.processor.util.DtoTypeUtils;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;
import org.pipelineframework.processor.util.PipelineContractMetadataGenerator;
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
    private final GenerationPathResolver generationPathResolver;
    private final ProtobufParserService protobufParserService;
    private final SideEffectBeanService sideEffectBeanService;
    private final StepArtifactGenerationService stepArtifactGenerationService;
    private final GenerationPolicy generationPolicy;

    /**
     * Creates a new PipelineGenerationPhase.
     */
    public PipelineGenerationPhase() {
        this(new GenerationPathResolver(), new GenerationPolicy());
    }

    PipelineGenerationPhase(GenerationPathResolver generationPathResolver, GenerationPolicy generationPolicy) {
        this.generationPathResolver = Objects.requireNonNull(generationPathResolver, "generationPathResolver");
        this.generationPolicy = Objects.requireNonNull(generationPolicy, "generationPolicy");
        this.sideEffectBeanService = new SideEffectBeanService(generationPathResolver);
        this.protobufParserService = new ProtobufParserService(generationPathResolver);
        this.stepArtifactGenerationService = new StepArtifactGenerationService(
            generationPathResolver, generationPolicy, sideEffectBeanService);
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
        generationPathResolver.resetGeneratedSourcesRoot(ctx);

        // Get the bindings map from the context
        Map<String, Object> bindingsMap = ctx.getRendererBindings();
        SpringRendererProfileSupport.validateGenerationSupported(ctx);
        boolean springProfile = SpringRendererProfileSupport.isSpringProfile(ctx);

        // Initialize renderers
        GrpcServiceAdapterRenderer grpcRenderer = new GrpcServiceAdapterRenderer(GenerationTarget.GRPC_SERVICE);
        org.pipelineframework.processor.renderer.ClientStepRenderer clientRenderer =
            new org.pipelineframework.processor.renderer.ClientStepRenderer(GenerationTarget.CLIENT_STEP);
        PipelineRenderer<LocalBinding> localClientRenderer = springProfile
            ? new SpringLocalClientStepRenderer()
            : new org.pipelineframework.processor.renderer.LocalClientStepRenderer();
        RestClientStepRenderer restClientRenderer = new RestClientStepRenderer();
        PipelineRenderer<RestBinding> restRenderer = springProfile
            ? new SpringRestResourceRenderer()
            : new RestResourceRenderer();
        RestFunctionHandlerRenderer restFunctionHandlerRenderer = new RestFunctionHandlerRenderer();
        BlockingReactiveBridgeRenderer blockingReactiveBridgeRenderer = new BlockingReactiveBridgeRenderer();
        RemoteOperatorAdapterRenderer remoteOperatorAdapterRenderer = new RemoteOperatorAdapterRenderer();
        AwaitClientStepRenderer awaitClientStepRenderer = new AwaitClientStepRenderer();
        QueryClientStepRenderer queryClientStepRenderer = new QueryClientStepRenderer();
        OrchestratorGrpcRenderer orchestratorGrpcRenderer = new OrchestratorGrpcRenderer();
        OrchestratorRestResourceRenderer orchestratorRestRenderer = new OrchestratorRestResourceRenderer();
        AbstractOrchestratorFunctionHandlerRenderer orchestratorFunctionHandlerRenderer =
            FunctionHandlerRendererFactory.createOrchestratorRenderer(ctx.getRendererProfile());
        OrchestratorCliRenderer orchestratorCliRenderer = new OrchestratorCliRenderer();
        OrchestratorIngestClientRenderer orchestratorIngestClientRenderer = new OrchestratorIngestClientRenderer();
        CheckpointPublicationDescriptorRenderer checkpointPublicationDescriptorRenderer =
            new CheckpointPublicationDescriptorRenderer();
        CheckpointSubscriptionHandlerRenderer checkpointSubscriptionHandlerRenderer =
            new CheckpointSubscriptionHandlerRenderer();
        ExternalAdapterRenderer externalAdapterRenderer = new ExternalAdapterRenderer(GenerationTarget.EXTERNAL_ADAPTER);
        ObjectIngestInputAdapterRenderer objectIngestInputAdapterRenderer = new ObjectIngestInputAdapterRenderer();
        TerminalOutputAdapterRenderer terminalOutputAdapterRenderer = new TerminalOutputAdapterRenderer();

        // Initialize role metadata generator
        RoleMetadataGenerator roleMetadataGenerator = new RoleMetadataGenerator(ctx.getProcessingEnv());
        PipelinePlatformMetadataGenerator platformMetadataGenerator =
            new PipelinePlatformMetadataGenerator(ctx.getProcessingEnv());

        // Get the cache key generator
        ClassName cacheKeyGenerator = resolveCacheKeyGenerator(ctx).orElse(null);

        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();
        generateCheckpointBoundaryArtifacts(
            ctx,
            checkpointPublicationDescriptorRenderer,
            checkpointSubscriptionHandlerRenderer,
            roleMetadataGenerator,
            cacheKeyGenerator,
            descriptorSet);
        generateObjectIngestInputAdapter(
            ctx,
            objectIngestInputAdapterRenderer,
            roleMetadataGenerator,
            cacheKeyGenerator,
            descriptorSet);

        if (ctx.getStepModels().isEmpty() && !ctx.isOrchestratorGenerated()) {
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
                    externalAdapterRenderer.render(
                        externalAdapterBinding,
                        createExternalAdapterGenerationContext(
                            ctx,
                            adapterRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                    String externalAdapterClassName = model.servicePackage() + ".pipeline."
                        + ExternalAdapterRenderer.getExternalAdapterClassName(model);
                    roleMetadataGenerator.recordClassWithRole(externalAdapterClassName, adapterRole.name());
                } catch (IOException e) {
                    if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                        ctx.getProcessingEnv().getMessager().printMessage(
                            javax.tools.Diagnostic.Kind.ERROR,
                            "Failed to generate external adapter for '" + model.serviceName() + "': " + e.getMessage());
                    } else {
                        LOG.errorf(e, "Failed to generate external adapter for '%s': %s", model.serviceName(), e.getMessage());
                    }
                }
                continue;
            }

            // Get the bindings for this model (for non-delegation steps)
            String grpcBindingKey = model.serviceName() + "_grpc";
            String restBindingKey = model.serviceName() + "_rest";
            String localBindingKey = model.serviceName() + "_local";

            Object grpcBindingObj = bindingsMap.get(grpcBindingKey);
            Object restBindingObj = bindingsMap.get(restBindingKey);
            Object localBindingObj = bindingsMap.get(localBindingKey);

            GrpcBinding grpcBinding = null;
            if (grpcBindingObj instanceof GrpcBinding value) {
                grpcBinding = value;
            } else if (grpcBindingObj != null) {
                LOG.warnf("Invalid binding type for '%s': expected GrpcBinding but got %s",
                    grpcBindingKey, grpcBindingObj.getClass().getName());
            }

            RestBinding restBinding = null;
            if (restBindingObj instanceof RestBinding value) {
                restBinding = value;
            } else if (restBindingObj != null) {
                LOG.warnf("Invalid binding type for '%s': expected RestBinding but got %s",
                    restBindingKey, restBindingObj.getClass().getName());
            }

            LocalBinding localBinding = null;
            if (localBindingObj instanceof LocalBinding value) {
                localBinding = value;
            } else if (localBindingObj != null) {
                LOG.warnf("Invalid binding type for '%s': expected LocalBinding but got %s",
                    localBindingKey, localBindingObj.getClass().getName());
            }

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
                restFunctionHandlerRenderer,
                    blockingReactiveBridgeRenderer,
                    remoteOperatorAdapterRenderer,
                    awaitClientStepRenderer,
                    queryClientStepRenderer);
        }

        if (ctx.isTransportModeGrpc() && descriptorSet != null) {
            protobufParserService.generateProtobufParsers(ctx, descriptorSet);
        }

        generateObjectPublishTerminalAdapter(
            ctx,
            terminalOutputAdapterRenderer,
            roleMetadataGenerator,
            cacheKeyGenerator,
            descriptorSet);

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
            PipelineOrderMetadataGenerator orderMetadataGenerator =
                new PipelineOrderMetadataGenerator(ctx.getProcessingEnv());
            orderMetadataGenerator.writeOrderMetadata(ctx);
            PipelineContractMetadataGenerator contractMetadataGenerator =
                new PipelineContractMetadataGenerator(ctx.getProcessingEnv());
            contractMetadataGenerator.writePipelineContract(ctx);
            if (ctx.isOrchestratorGenerated()) {
                PipelineTelemetryMetadataGenerator telemetryMetadataGenerator =
                    new PipelineTelemetryMetadataGenerator(ctx.getProcessingEnv());
                telemetryMetadataGenerator.writeTelemetryMetadata(ctx);
                CheckpointHandoffMetadataGenerator handoffMetadataGenerator =
                    new CheckpointHandoffMetadataGenerator(ctx.getProcessingEnv());
                handoffMetadataGenerator.writeHandoffMetadata(ctx);
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

    private void generateObjectPublishTerminalAdapter(
        PipelineCompilationContext ctx,
        TerminalOutputAdapterRenderer renderer,
        RoleMetadataGenerator roleMetadataGenerator,
        ClassName cacheKeyGenerator,
        DescriptorProtos.FileDescriptorSet descriptorSet
    ) {
        Optional<ObjectPublishGenerationConfig> objectPublishConfig = objectPublishGenerationConfig(ctx);
        if (objectPublishConfig.isEmpty() || ctx.isTransportModeLocal() || ctx.isPluginHost()) {
            return;
        }
        PipelineStepModel terminalModel = terminalBusinessStepWithOutputMapper(ctx)
            .orElseThrow(() -> new IllegalStateException(
                "Object Publish requires a terminal business step with an outbound mapper"));
        TypeName domainType = terminalModel.outputMapping().domainType();
        TypeName mapperType = terminalModel.outputMapping().mapperType();
        TypeName externalType = objectPublishExternalType(ctx, terminalModel);
        GenerationContext adapterContext = new GenerationContext(
            ctx.getProcessingEnv(),
            generationPathResolver.resolveRoleOutputDir(ctx, resolveClientRole(terminalModel.deploymentRole())),
            resolveClientRole(terminalModel.deploymentRole()),
            Set.of(),
            cacheKeyGenerator,
            descriptorSet,
            ctx.getTransportMode(),
            objectPublishConfig.get().basePackage());
        try {
            ClassName generatedClass = renderer.render(
                objectPublishConfig.get().basePackage(), domainType, externalType, mapperType, adapterContext);
            roleMetadataGenerator.recordClassWithRole(
                generatedClass.canonicalName(),
                resolveClientRole(terminalModel.deploymentRole()).name());
        } catch (IOException | RuntimeException e) {
            String message = "Failed to generate Object Publish terminal output adapter: " + e.getMessage();
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR, message);
            }
            throw new RuntimeException(message, e);
        }
    }

    private void generateObjectIngestInputAdapter(
        PipelineCompilationContext ctx,
        ObjectIngestInputAdapterRenderer renderer,
        RoleMetadataGenerator roleMetadataGenerator,
        ClassName cacheKeyGenerator,
        DescriptorProtos.FileDescriptorSet descriptorSet
    ) {
        Optional<ObjectIngestGenerationConfig> objectIngestConfig = objectIngestGenerationConfig(ctx);
        if (objectIngestConfig.isEmpty() || ctx.isTransportModeLocal() || ctx.isPluginHost()) {
            return;
        }
        PipelineStepModel firstModel = firstBusinessStepWithInputMapper(ctx)
            .orElseThrow(() -> new IllegalStateException(
                "Object Ingest requires the first business step to declare an inbound mapper"));
        TypeName domainType = firstModel.inputMapping().domainType();
        TypeName mapperType = firstModel.inputMapping().mapperType();
        TypeName externalType = objectIngestExternalType(ctx, firstModel);
        GenerationContext adapterContext = new GenerationContext(
            ctx.getProcessingEnv(),
            generationPathResolver.resolveRoleOutputDir(ctx, resolveClientRole(firstModel.deploymentRole())),
            resolveClientRole(firstModel.deploymentRole()),
            Set.of(),
            cacheKeyGenerator,
            descriptorSet,
            ctx.getTransportMode(),
            objectIngestConfig.get().basePackage());
        try {
            ClassName generatedClass = renderer.render(
                objectIngestConfig.get().basePackage(), domainType, externalType, mapperType, adapterContext);
            roleMetadataGenerator.recordClassWithRole(
                generatedClass.canonicalName(),
                resolveClientRole(firstModel.deploymentRole()).name());
        } catch (IOException | RuntimeException e) {
            String message = "Failed to generate Object Ingest input adapter: " + e.getMessage();
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR, message);
            }
            throw new RuntimeException(message, e);
        }
    }

    private Optional<ObjectPublishGenerationConfig> objectPublishGenerationConfig(PipelineCompilationContext ctx) {
        if (ctx.getPipelineTemplateConfig() instanceof org.pipelineframework.config.template.PipelineTemplateConfig templateConfig) {
            if (templateConfig.output() != null && templateConfig.output().object() != null) {
                return Optional.of(new ObjectPublishGenerationConfig(templateConfig.basePackage()));
            }
        }
        return loadPipelineYamlConfig(ctx)
            .filter(yamlConfig -> yamlConfig.output() != null && yamlConfig.output().object() != null)
            .map(yamlConfig -> new ObjectPublishGenerationConfig(yamlConfig.basePackage()));
    }

    private Optional<ObjectIngestGenerationConfig> objectIngestGenerationConfig(PipelineCompilationContext ctx) {
        if (ctx.getPipelineTemplateConfig() instanceof org.pipelineframework.config.template.PipelineTemplateConfig templateConfig) {
            if (templateConfig.input() != null && templateConfig.input().object() != null) {
                return Optional.of(new ObjectIngestGenerationConfig(templateConfig.basePackage()));
            }
        }
        return loadPipelineYamlConfig(ctx)
            .filter(yamlConfig -> yamlConfig.input() != null && yamlConfig.input().object() != null)
            .map(yamlConfig -> new ObjectIngestGenerationConfig(yamlConfig.basePackage()));
    }

    private Optional<org.pipelineframework.config.pipeline.PipelineYamlConfig> loadPipelineYamlConfig(PipelineCompilationContext ctx) {
        Optional<java.nio.file.Path> configPath = resolvePipelineConfigPath(ctx);
        if (configPath.isEmpty()) {
            return Optional.empty();
        }
        org.pipelineframework.config.pipeline.PipelineYamlConfigLoader loader = ctx.getProcessingEnv() != null
            ? new org.pipelineframework.config.pipeline.PipelineYamlConfigLoader(ctx.getProcessingEnv().getOptions()::get, System::getenv)
            : new org.pipelineframework.config.pipeline.PipelineYamlConfigLoader(key -> null, System::getenv);
        return Optional.of(loader.load(configPath.get()));
    }

    private Optional<java.nio.file.Path> resolvePipelineConfigPath(PipelineCompilationContext ctx) {
        Map<String, String> options = ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getOptions() : Map.of();
        String explicit = options.get("pipeline.config");
        if (explicit != null && !explicit.isBlank()) {
            java.nio.file.Path explicitPath = java.nio.file.Path.of(explicit.trim());
            if (!explicitPath.isAbsolute()) {
                if (ctx.getModuleDir() == null) {
                    return Optional.empty();
                }
                explicitPath = ctx.getModuleDir().resolve(explicitPath).normalize();
            }
            if (java.nio.file.Files.exists(explicitPath)) {
                return Optional.of(explicitPath);
            }
        }
        if (ctx.getModuleDir() == null) {
            return Optional.empty();
        }
        return new org.pipelineframework.config.pipeline.PipelineYamlConfigLocator().locate(ctx.getModuleDir());
    }

    private record ObjectPublishGenerationConfig(String basePackage) {
    }

    private record ObjectIngestGenerationConfig(String basePackage) {
    }

    private Optional<PipelineStepModel> firstBusinessStepWithInputMapper(PipelineCompilationContext ctx) {
        List<PipelineStepModel> models = ctx.getStepModels() == null ? List.of() : ctx.getStepModels();
        for (PipelineStepModel model : models) {
            if (model != null
                && !model.sideEffect()
                && model.inputMapping() != null
                && model.inputMapping().mapperType() != null) {
                return Optional.of(model);
            }
        }
        return Optional.empty();
    }

    private Optional<PipelineStepModel> terminalBusinessStepWithOutputMapper(PipelineCompilationContext ctx) {
        List<PipelineStepModel> models = ctx.getStepModels() == null ? List.of() : ctx.getStepModels();
        for (int i = models.size() - 1; i >= 0; i--) {
            PipelineStepModel model = models.get(i);
            if (model != null
                && !model.sideEffect()
                && model.outputMapping() != null
                && model.outputMapping().mapperType() != null) {
                return Optional.of(model);
            }
        }
        return Optional.empty();
    }

    private TypeName objectPublishExternalType(
        PipelineCompilationContext ctx,
        PipelineStepModel terminalModel
    ) {
        if (ctx.isTransportModeRest()) {
            return DtoTypeUtils.toDtoType(terminalModel.outputMapping().domainType());
        }
        Object binding = ctx.getRendererBindings().get(terminalModel.serviceName() + "_grpc");
        if (binding instanceof GrpcBinding grpcBinding) {
            if (ctx.getProcessingEnv() == null) {
                throw new IllegalStateException(
                    "Object Publish terminal adapter requires a processing environment in gRPC mode");
            }
            return new GrpcJavaTypeResolver().resolve(grpcBinding, ctx.getProcessingEnv().getMessager()).grpcReturnType();
        }
        throw new IllegalStateException(
            "Object Publish terminal adapter requires a gRPC binding for step " + terminalModel.serviceName());
    }

    private TypeName objectIngestExternalType(
        PipelineCompilationContext ctx,
        PipelineStepModel firstModel
    ) {
        if (ctx.isTransportModeRest()) {
            return DtoTypeUtils.toDtoType(firstModel.inputMapping().domainType());
        }
        Object binding = ctx.getRendererBindings().get(firstModel.serviceName() + "_grpc");
        if (binding instanceof GrpcBinding grpcBinding) {
            if (ctx.getProcessingEnv() == null) {
                throw new IllegalStateException(
                    "Object Ingest input adapter requires a processing environment in gRPC mode");
            }
            return new GrpcJavaTypeResolver().resolve(grpcBinding, ctx.getProcessingEnv().getMessager()).grpcParameterType();
        }
        throw new IllegalStateException(
            "Object Ingest input adapter requires a gRPC binding for step " + firstModel.serviceName());
    }

    private void generateCheckpointBoundaryArtifacts(
        PipelineCompilationContext ctx,
        CheckpointPublicationDescriptorRenderer checkpointPublicationDescriptorRenderer,
        CheckpointSubscriptionHandlerRenderer checkpointSubscriptionHandlerRenderer,
        RoleMetadataGenerator roleMetadataGenerator,
        ClassName cacheKeyGenerator,
        DescriptorProtos.FileDescriptorSet descriptorSet
    ) {
        if (!(ctx.getPipelineTemplateConfig() instanceof org.pipelineframework.config.template.PipelineTemplateConfig templateConfig)) {
            return;
        }
        Object orchestratorBindingObj = ctx.getRendererBindings().get(ORCHESTRATOR_BINDING_KEY);
        if (!(orchestratorBindingObj instanceof OrchestratorBinding orchestratorBinding)) {
            return;
        }
        if (templateConfig.output() != null && templateConfig.output().checkpoint() != null) {
            GenerationContext publicationContext = new GenerationContext(
                ctx.getProcessingEnv(),
                generationPathResolver.resolveRoleOutputDir(ctx, DeploymentRole.PIPELINE_SERVER),
                DeploymentRole.PIPELINE_SERVER,
                Set.of(),
                cacheKeyGenerator,
                descriptorSet);
            try {
                TypeName publicationPayloadType = ctx.getStepModels().isEmpty()
                    ? null
                    : ctx.getStepModels().getLast().outputMapping().domainType();
                ClassName generatedClass = checkpointPublicationDescriptorRenderer.render(
                    templateConfig.basePackage(),
                    templateConfig.output().checkpoint(),
                    publicationPayloadType,
                    publicationContext);
                roleMetadataGenerator.recordClassWithRole(generatedClass, DeploymentRole.PIPELINE_SERVER.name());
            } catch (IOException | RuntimeException e) {
                String message = "Failed to generate checkpoint publication descriptor in base package '"
                    + templateConfig.basePackage()
                    + "': "
                    + e.getMessage();
                if (ctx.getProcessingEnv() != null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.ERROR,
                        message);
                }
                throw new RuntimeException(message, e);
            }
        }
        if (templateConfig.input() != null && templateConfig.input().subscription() != null) {
            GenerationContext handlerContext = new GenerationContext(
                ctx.getProcessingEnv(),
                generationPathResolver.resolveRoleOutputDir(ctx, DeploymentRole.PIPELINE_SERVER),
                DeploymentRole.PIPELINE_SERVER,
                Set.of(),
                cacheKeyGenerator,
                descriptorSet);
            try {
                ClassName generatedClass = checkpointSubscriptionHandlerRenderer.render(
                    orchestratorBinding,
                    templateConfig.input().subscription(),
                    handlerContext);
                roleMetadataGenerator.recordClassWithRole(generatedClass, DeploymentRole.PIPELINE_SERVER.name());
            } catch (IOException | RuntimeException e) {
                String message = "Failed to generate checkpoint subscription artifacts in base package '"
                    + templateConfig.basePackage()
                    + "': "
                    + e.getMessage();
                if (ctx.getProcessingEnv() != null) {
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.ERROR,
                        message);
                }
                throw new RuntimeException(message, e);
            }
        }
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
            PipelineRenderer<LocalBinding> localClientRenderer,
            RestClientStepRenderer restClientRenderer,
            PipelineRenderer<RestBinding> restRenderer,
            RestFunctionHandlerRenderer restFunctionHandlerRenderer,
            BlockingReactiveBridgeRenderer blockingReactiveBridgeRenderer,
            RemoteOperatorAdapterRenderer remoteOperatorAdapterRenderer,
            AwaitClientStepRenderer awaitClientStepRenderer,
            QueryClientStepRenderer queryClientStepRenderer) throws IOException {
        stepArtifactGenerationService.generateArtifactsForModel(
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
            restFunctionHandlerRenderer,
            blockingReactiveBridgeRenderer,
            remoteOperatorAdapterRenderer,
            awaitClientStepRenderer,
            queryClientStepRenderer);
    }

    /**
     * Collects enabled aspect names from the compilation context and normalizes them to lowercase.
     *
     * @param ctx the pipeline compilation context to read aspect models from
     * @return an unmodifiable set of enabled aspect names in lowercase; empty if no aspects are present
     */
    private Set<String> computeEnabledAspects(PipelineCompilationContext ctx) {
        return Optional.ofNullable(ctx.getAspectModels())
            .orElse(List.of())
            .stream()
            .filter(Objects::nonNull)
            .map(PipelineAspectModel::name)
            .filter(Objects::nonNull)
            .map(aspectName -> aspectName.toLowerCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Creates a GenerationContext configured for producing an external adapter for the given deployment role.
     *
     * @param ctx the pipeline compilation context providing the processing environment and generation root
     * @param adapterRole the deployment role that determines the adapter's output directory and target role
     * @param enabledAspects set of enabled aspect names (lowercase); may be empty
     * @param cacheKeyGenerator a ClassName for a cache key generator to include, or {@code null} if none
     * @param descriptorSet optional protobuf FileDescriptorSet to include, or {@code null}
     * @return a GenerationContext configured for external adapter generation for the specified role
     */
    private GenerationContext createExternalAdapterGenerationContext(
            PipelineCompilationContext ctx,
            org.pipelineframework.processor.ir.DeploymentRole adapterRole,
            Set<String> enabledAspects,
            ClassName cacheKeyGenerator,
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        return new GenerationContext(
            ctx.getProcessingEnv(),
            resolveRoleOutputDir(ctx, adapterRole),
            adapterRole,
            enabledAspects,
            cacheKeyGenerator,
            descriptorSet);
    }

    /**
     * Generate orchestrator server and client source artifacts based on the configured orchestrator binding.
     *
     * When the binding transport is REST, emits REST server sources and, if platform-function mode applies and the
     * binding is non-streaming, also emits native function handlers and records their handler classes in role metadata.
     * When the transport is gRPC or unspecified, emits a gRPC server. Server generation is skipped for LOCAL transport.
     * Additionally emits a CLI client when {@code generateCli} is true and emits an ingest client when the transport is
     * neither REST nor LOCAL. Generation I/O failures are reported to the processing environment messager.
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
            AbstractOrchestratorFunctionHandlerRenderer orchestratorFunctionHandlerRenderer,
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
                        orchestratorFunctionHandlerRenderer.handlerFqcn(binding.basePackage()),
                        role.name());
                    roleMetadataGenerator.recordClassWithRole(
                        orchestratorFunctionHandlerRenderer.runAsyncHandlerFqcn(binding.basePackage()),
                        role.name());
                    roleMetadataGenerator.recordClassWithRole(
                        orchestratorFunctionHandlerRenderer.statusHandlerFqcn(binding.basePackage()),
                        role.name());
                    roleMetadataGenerator.recordClassWithRole(
                        orchestratorFunctionHandlerRenderer.resultHandlerFqcn(binding.basePackage()),
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
     * @return the configured cache key generator class name
     */
    private Optional<ClassName> resolveCacheKeyGenerator(PipelineCompilationContext ctx) {
        if (ctx.getProcessingEnv() == null) {
            return Optional.empty();
        }
        String configured = ctx.getProcessingEnv().getOptions().get("pipeline.cache.keyGenerator");
        if (configured == null || configured.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(ClassName.bestGuess(configured));
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
        org.pipelineframework.processor.ir.DeploymentRole mapped = generationPolicy.resolveClientRole(serverRole);
        return mapped != null ? mapped : org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
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
        return generationPathResolver.resolveRoleOutputDir(ctx, role);
    }
}
