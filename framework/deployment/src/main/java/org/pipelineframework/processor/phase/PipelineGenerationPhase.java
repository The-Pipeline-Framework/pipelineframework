package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
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
     * Generate pipeline source artifacts and metadata for all configured step models and the orchestrator.
     *
     * <p>Produces per-step artifacts (gRPC services, REST resources, client classes, side-effect beans), optionally
     * generates protobuf parsers when applicable, and writes role, platform, and orchestrator-related metadata.</p>
     *
     * @param ctx the compilation context containing step models, renderer bindings, descriptor set, processing environment, and generation settings
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
        AbstractFunctionHandlerRenderer restFunctionHandlerRenderer = FunctionHandlerRendererFactory.createRenderer();
        AbstractOrchestratorFunctionHandlerRenderer orchestratorFunctionHandlerRenderer = FunctionHandlerRendererFactory.createOrchestratorRenderer();
        RemoteOperatorAdapterRenderer remoteOperatorAdapterRenderer = new RemoteOperatorAdapterRenderer();
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
                remoteOperatorAdapterRenderer);
        }

        if (ctx.isTransportModeGrpc() && descriptorSet != null) {
            protobufParserService.generateProtobufParsers(ctx, descriptorSet);
        }

        // Generate orchestrator artifacts if needed
        if (ctx.isOrchestratorGenerated()) {
            OrchestratorBinding orchestratorBinding = (OrchestratorBinding) bindingsMap.get(ORCHESTRATOR_BINDING_KEY);
            if (orchestratorBinding != null) {
                generateOrchestratorServer(
                    ctx,
                    descriptorSet,
                    orchestratorBinding.cliName() != null,
                    orchestratorFunctionHandlerRenderer,
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
     * Generate all source and metadata artifacts for a single pipeline step.
     *
     * @param ctx the compilation context with environment, options, and models
     * @param model the pipeline step model to generate artifacts for
     * @param grpcBinding gRPC binding for the step; may be null when gRPC is not targeted
     * @param restBinding REST binding for the step; may be null when REST is not targeted
     * @param localBinding local-transport binding for the step; may be null when local transport is not targeted
     * @param generatedSideEffectBeans set tracking keys of already-generated side-effect CDI beans to avoid duplicates
     * @param enabledAspects set of enabled aspect names (lowercased)
     * @param descriptorSet protobuf descriptor set used for protobuf-related generation; may be null
     * @param cacheKeyGenerator optional ClassName of a cache key generator to include in generation contexts; may be null
     * @param roleMetadataGenerator recorder used to register generated class names with their deployment roles
     * @param grpcRenderer renderer for producing gRPC service classes
     * @param clientRenderer renderer for producing gRPC client step classes
     * @param localClientRenderer renderer for producing local-transport client step classes
     * @param restClientRenderer renderer for producing REST client step classes
     * @param restRenderer renderer for producing REST resource classes
     * @param restFunctionHandlerRenderer renderer for producing function handlers for unary REST resources
     * @param remoteOperatorAdapterRenderer renderer for producing remote operator adapter classes
     * @throws IOException if an I/O error occurs while writing generated sources
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
            AbstractFunctionHandlerRenderer restFunctionHandlerRenderer,
            RemoteOperatorAdapterRenderer remoteOperatorAdapterRenderer) throws IOException {
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
            remoteOperatorAdapterRenderer);
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
     * Generate orchestrator server and client source artifacts according to the configured orchestrator binding.
     *
     * When the binding transport is REST, emits a REST resource and, if running in function platform mode with
     * non-streaming input/output, emits native function handlers and records their handler classes in role metadata.
     * When the transport is GRPC (and not LOCAL), emits a gRPC server. Server generation is skipped for LOCAL transport.
     * Emits a CLI client when {@code generateCli} is true and emits an ingest client when the transport is neither REST
     * nor LOCAL. I/O failures during generation are reported to the processing environment messager.
     *
     * @param ctx the pipeline compilation context
     * @param descriptorSet protobuf descriptor set used during generation; may be {@code null}
     * @param generateCli whether to generate the CLI orchestrator client
     * @param orchestratorFunctionHandlerRenderer renderer for native function orchestrator handlers
     * @param roleMetadataGenerator generator that records generated classes by deployment role
     * @param cacheKeyGenerator optional cache key generator class; may be {@code null}
     */
    private void generateOrchestratorServer(
            PipelineCompilationContext ctx,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            boolean generateCli,
            AbstractOrchestratorFunctionHandlerRenderer orchestratorFunctionHandlerRenderer,
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
            boolean grpc = "GRPC".equalsIgnoreCase(transport);
            
            // Generate REST resource for REST transport
            if (rest) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.REST_SERVER;
                org.pipelineframework.processor.renderer.OrchestratorRestResourceRenderer orchestratorRestRenderer = 
                    new org.pipelineframework.processor.renderer.OrchestratorRestResourceRenderer();
                orchestratorRestRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
                
                // Generate function handlers for FUNCTION platform mode (unary only)
                if (ctx.isPlatformModeFunction() && !binding.inputStreaming() && !binding.outputStreaming()) {
                    orchestratorFunctionHandlerRenderer.render(binding, new GenerationContext(
                        ctx.getProcessingEnv(),
                        resolveRoleOutputDir(ctx, role),
                        role,
                        java.util.Set.of(),
                        cacheKeyGenerator,
                        descriptorSet));
                    String basePackage = binding.basePackage();
                    roleMetadataGenerator.recordClassWithRole(
                        basePackage + ".orchestrator.service." + AbstractOrchestratorFunctionHandlerRenderer.HANDLER_CLASS,
                        role.name());
                    roleMetadataGenerator.recordClassWithRole(
                        basePackage + ".orchestrator.service." + AbstractOrchestratorFunctionHandlerRenderer.RUN_ASYNC_HANDLER_CLASS,
                        role.name());
                    roleMetadataGenerator.recordClassWithRole(
                        basePackage + ".orchestrator.service." + AbstractOrchestratorFunctionHandlerRenderer.STATUS_HANDLER_CLASS,
                        role.name());
                    roleMetadataGenerator.recordClassWithRole(
                        basePackage + ".orchestrator.service." + AbstractOrchestratorFunctionHandlerRenderer.RESULT_HANDLER_CLASS,
                        role.name());
                } else if (ctx.isPlatformModeFunction()) {
                    // FUNCTION platform mode requires unary input/output for orchestrator handlers
                    throw new IllegalStateException(
                        "Orchestrator function handlers require unary input and output. " +
                        "Got transport=" + transport + ", platform=FUNCTION, " +
                        "streamingInput=" + binding.inputStreaming() + ", streamingOutput=" + binding.outputStreaming());
                }
            }
            
            // Generate gRPC service for GRPC transport (non-LOCAL)
            if (grpc && !local) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER;
                org.pipelineframework.processor.renderer.OrchestratorGrpcRenderer orchestratorGrpcRenderer = 
                    new org.pipelineframework.processor.renderer.OrchestratorGrpcRenderer();
                orchestratorGrpcRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
            }

            // Generate CLI client if requested
            if (generateCli) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
                org.pipelineframework.processor.renderer.OrchestratorCliRenderer orchestratorCliRenderer = 
                    new org.pipelineframework.processor.renderer.OrchestratorCliRenderer();
                orchestratorCliRenderer.render(binding, new GenerationContext(
                    ctx.getProcessingEnv(),
                    resolveRoleOutputDir(ctx, role),
                    role,
                    java.util.Set.of(),
                    cacheKeyGenerator,
                    descriptorSet));
            }

            // Generate ingest client for GRPC transport (non-LOCAL, non-REST)
            if (!rest && !local) {
                org.pipelineframework.processor.ir.DeploymentRole role = org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT;
                org.pipelineframework.processor.renderer.OrchestratorIngestClientRenderer orchestratorIngestClientRenderer = 
                    new org.pipelineframework.processor.renderer.OrchestratorIngestClientRenderer();
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
