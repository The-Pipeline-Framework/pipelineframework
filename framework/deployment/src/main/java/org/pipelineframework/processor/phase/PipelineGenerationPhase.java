package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.*;

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
                    ctx.getProcessingEnv().getMessager().printMessage(
                        javax.tools.Diagnostic.Kind.ERROR,
                        "Failed to generate external adapter for '" + model.serviceName() + "': " + e.getMessage());
                }
                continue;
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
                restFunctionHandlerRenderer);
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
            org.pipelineframework.processor.renderer.LocalClientStepRenderer localClientRenderer,
            RestClientStepRenderer restClientRenderer,
            RestResourceRenderer restRenderer,
            RestFunctionHandlerRenderer restFunctionHandlerRenderer) throws IOException {
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
            restFunctionHandlerRenderer);
    }

    private Set<String> computeEnabledAspects(PipelineCompilationContext ctx) {
        return Optional.ofNullable(ctx.getAspectModels())
            .orElse(List.of())
            .stream()
            .filter(Objects::nonNull)
            .map(PipelineAspectModel::name)
            .filter(Objects::nonNull)
            .map(aspectName -> aspectName.toLowerCase(Locale.ROOT))
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

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
