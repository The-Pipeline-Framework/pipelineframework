package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.*;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.config.PipelineAspectConfigLoader;
import org.pipelineframework.processor.config.PipelineStepConfigLoader;
import org.pipelineframework.processor.extractor.PipelineStepIRExtractor;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.renderer.*;
import org.pipelineframework.processor.util.*;
import org.pipelineframework.processor.validator.PipelineStepValidator;

/**
 * Java annotation processor that generates both gRPC client and server step implementations
 * based on @PipelineStep annotated service classes.
 * <p>
 * This class now serves as a thin dispatcher that coordinates with specialized
 * components for IR extraction, validation, and code generation.
 */
@SuppressWarnings("unused")
@SupportedAnnotationTypes({
    "org.pipelineframework.annotation.PipelineStep",
    "org.pipelineframework.annotation.PipelinePlugin",
    "org.pipelineframework.annotation.PipelineOrchestrator"
})
@SupportedOptions({
    "protobuf.descriptor.path",  // Optional: path to directory containing descriptor files
    "protobuf.descriptor.file",  // Optional: path to a specific descriptor file
    "pipeline.generatedSourcesDir", // Optional: base directory for role-specific generated sources
    "pipeline.generatedSourcesRoot", // Optional: legacy alias for generated sources base directory
    "pipeline.cache.keyGenerator", // Optional: fully-qualified CacheKeyGenerator class for @CacheResult
    "pipeline.orchestrator.generate" // Optional: enable orchestrator endpoint generation
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class PipelineStepProcessor extends AbstractProcessingTool {

    /**
     * Creates a new PipelineStepProcessor.
     *
     * <p>Constructs an instance without configuring internal components; call {@link #init(javax.annotation.processing.ProcessingEnvironment)}
     * with a ProcessingEnvironment before using the processor.</p>
     */
    public PipelineStepProcessor() {
    }

    /**
     * Suffix to append to generated gRPC client step classes.
     */
    public static final String GRPC_CLIENT_STEP_SUFFIX = "GrpcClientStep";

    /**
     * Suffix to append to generated REST client step classes.
     */
    public static final String REST_CLIENT_STEP_SUFFIX = "RestClientStep";

    /**
     * Suffix to append to generated gRPC service classes.
     */
    public static final String GRPC_SERVICE_SUFFIX = "GrpcService";

    /**
     * Package suffix for generated pipeline classes.
     */
    public static final String PIPELINE_PACKAGE_SUFFIX = ".pipeline";

    /**
     * Suffix to append to generated REST resource classes.
     */
    public static final String REST_RESOURCE_SUFFIX = "Resource";
    private static final String ORCHESTRATOR_SERVICE = "OrchestratorService";

    private PipelineStepIRExtractor irExtractor;
    private PipelineStepValidator validator;
    private GrpcServiceAdapterRenderer grpcRenderer;
    private ClientStepRenderer clientRenderer;
    private RestClientStepRenderer restClientRenderer;
    private RestResourceRenderer restRenderer;
    private OrchestratorGrpcRenderer orchestratorGrpcRenderer;
    private OrchestratorRestResourceRenderer orchestratorRestRenderer;
    private OrchestratorCliRenderer orchestratorCliRenderer;
    private GrpcBindingResolver bindingResolver;
    private RestBindingResolver restBindingResolver;
    private RoleMetadataGenerator roleMetadataGenerator;
    private Path generatedSourcesRoot;
    private java.util.List<org.pipelineframework.processor.ir.PipelineAspectModel> pipelineAspects = java.util.List.of();
    private boolean pluginHost;
    private boolean orchestratorGenerated;
    private PipelineTemplateConfig pipelineTemplateConfig;
    private final java.util.Set<String> generatedSideEffectBeanKeys = new java.util.HashSet<>();
    private TransportMode transportMode = TransportMode.GRPC;

    private enum TransportMode {
        GRPC,
        REST
    }

    /**
     * Initialises the processor and its helper components using the provided processing environment.
     * <p>
     * Initialises the IR extractor, validator, gRPC/client/REST renderers, binding resolvers and
     * the role metadata generator so the processor is ready to perform annotation processing.
     *
     * @param processingEnv the processing environment used to configure compiler-facing utilities
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        this.irExtractor = new PipelineStepIRExtractor(processingEnv);
        this.validator = new PipelineStepValidator(processingEnv);
        this.grpcRenderer = new GrpcServiceAdapterRenderer(GenerationTarget.GRPC_SERVICE);
        this.clientRenderer = new ClientStepRenderer(GenerationTarget.CLIENT_STEP);
        this.restClientRenderer = new RestClientStepRenderer();
        this.restRenderer = new RestResourceRenderer();
        this.orchestratorGrpcRenderer = new OrchestratorGrpcRenderer();
        this.orchestratorRestRenderer = new OrchestratorRestResourceRenderer();
        this.orchestratorCliRenderer = new OrchestratorCliRenderer();
        this.bindingResolver = new GrpcBindingResolver();
        this.restBindingResolver = new RestBindingResolver();
        this.roleMetadataGenerator = new RoleMetadataGenerator(processingEnv);
        this.generatedSourcesRoot = resolveGeneratedSourcesRoot(processingEnv);
        this.pipelineAspects = loadPipelineAspects(processingEnv);
        this.pipelineTemplateConfig = loadPipelineTemplateConfig(processingEnv);
        this.transportMode = loadPipelineTransport(processingEnv);
    }

    /**
     * Coordinates extraction, validation and artifact generation for elements annotated with {@code @PipelineStep}.
     *
     * <p>For each annotated class this method extracts a pipeline model, validates it, resolves gRPC/REST bindings
     * as required and orchestrates generation of the corresponding artifacts. When processing is finished it writes
     * role metadata.</p>
     *
     * @param annotations the annotation types requested to be processed in this round
     * @param roundEnv the environment for information about the current and prior round
     * @return {@code true} if at least one annotation was processed, {@code false} when no annotations were present
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        java.util.Set<? extends Element> pipelineStepElements =
            roundEnv.getElementsAnnotatedWith(PipelineStep.class);
        java.util.Set<? extends Element> orchestratorElements =
            roundEnv.getElementsAnnotatedWith(PipelineOrchestrator.class);
        boolean generateOrchestrator = shouldGenerateOrchestrator(orchestratorElements);

        if (pipelineStepElements.isEmpty() && !generateOrchestrator) {
            if (annotations.isEmpty()) {
                // Only write metadata when no more annotations to process (end of last round)
                if (roundEnv.processingOver()) {
                    try {
                        roleMetadataGenerator.writeRoleMetadata();
                    } catch (IOException e) {
                        processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR,
                            "Failed to write role metadata: " + e.getMessage());
                    }
                }
            }
            return false;
        }

        java.util.Set<? extends Element> pluginElements = roundEnv.getElementsAnnotatedWith(PipelinePlugin.class);
        pluginHost = !pluginElements.isEmpty();
        java.util.Set<String> pluginAspectNames = new java.util.LinkedHashSet<>();
        for (Element element : pluginElements) {
            AnnotationMirror annotationMirror = AnnotationProcessingUtils.getAnnotationMirror(element, PipelinePlugin.class);
            if (annotationMirror == null) {
                continue;
            }
            String aspectName = AnnotationProcessingUtils.getAnnotationValueAsString(annotationMirror, "value", null);
            if (aspectName != null && !aspectName.isBlank()) {
                pluginAspectNames.add(aspectName.trim());
            }
        }

        java.util.Set<String> expectedServiceNames = new java.util.LinkedHashSet<>();
        for (Element element : pipelineStepElements) {
            if (element.getKind() == ElementKind.CLASS) {
                expectedServiceNames.add(element.getSimpleName().toString());
            }
        }
        if (generateOrchestrator) {
            expectedServiceNames.add("OrchestratorService");
        }

        // Locate and load FileDescriptorSet from protobuf compilation
        DescriptorProtos.FileDescriptorSet descriptorSet = null;
        if (transportMode == TransportMode.GRPC && (!pipelineStepElements.isEmpty() || generateOrchestrator)) {
            DescriptorFileLocator descriptorLocator = new DescriptorFileLocator();
            try {
                descriptorSet = descriptorLocator.locateAndLoadDescriptors(
                    processingEnv.getOptions(),
                    expectedServiceNames,
                    processingEnv.getMessager());
            } catch (IOException e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                    "Failed to load protobuf FileDescriptorSet: " + e.getMessage() +
                    ". gRPC generation will fail for services that require descriptor-based resolution. " +
                    "Please ensure protobuf compilation happens before annotation processing.");
            }
        }

        java.util.List<ResolvedStep> resolvedSteps = new java.util.ArrayList<>();

        boolean allowClientTargets = orchestratorElements != null && !orchestratorElements.isEmpty();

        for (Element annotatedElement : pipelineStepElements) {
            if (annotatedElement.getKind() != ElementKind.CLASS) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "@PipelineStep can only be applied to classes", annotatedElement);
                continue;
            }

            TypeElement serviceClass = (TypeElement) annotatedElement;

            // Extract semantic information into model and binding
            var result = irExtractor.extract(serviceClass);
            if (result == null) {
                continue;
            }
            PipelineStepModel model = applyTransportTargets(result.model(), allowClientTargets);

            // Validate the model for semantic consistency
            if (!validator.validate(model, serviceClass)) {
                continue;
            }

            GrpcBinding grpcBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP)) {
                try {
                    grpcBinding = bindingResolver.resolve(model, descriptorSet);
                } catch (Exception e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                        "Failed to resolve gRPC binding for service '" + model.serviceName() + "': " + e.getMessage() +
                        ". This indicates a mismatch between the @PipelineStep annotation and the protobuf definition.");
                    continue;
                }
            }

            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, processingEnv);
            }

            ResolvedStep resolvedStep = new ResolvedStep(model, grpcBinding, restBinding);
            resolvedSteps.add(resolvedStep);
        }

        java.util.List<PipelineAspectModel> aspectsForExpansion = pipelineAspects.stream()
            .filter(aspect -> !isCacheAspect(aspect))
            .toList();

        java.util.List<ResolvedStep> generationOrder = resolvedSteps;
        if (!aspectsForExpansion.isEmpty()) {
            AspectExpansionProcessor expansionProcessor = new AspectExpansionProcessor();
            generationOrder = expansionProcessor.expandAspects(resolvedSteps, aspectsForExpansion);
        }

        if (pluginHost) {
            generationOrder = new java.util.ArrayList<>(generationOrder);
            generationOrder.addAll(buildPluginHostSteps(aspectsForExpansion, pluginAspectNames));
        }

        for (ResolvedStep resolvedStep : generationOrder) {
            PipelineStepModel model = applyTransportTargets(resolvedStep.model(), allowClientTargets);
            GrpcBinding grpcBinding = rebuildGrpcBinding(resolvedStep.grpcBinding(), model);
            RestBinding restBinding = rebuildRestBinding(resolvedStep.restBinding(), model);

            if (grpcBinding == null
                && (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                    || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP))) {
                try {
                    grpcBinding = bindingResolver.resolve(model, descriptorSet);
                } catch (Exception e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                        "Failed to resolve gRPC binding for '" + model.generatedName() + "': " +
                            e.getMessage() + ". Skipping generation for this step.");
                    continue;
                }
            }
            if (restBinding == null
                && (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                    || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP))) {
                restBinding = restBindingResolver.resolve(model, processingEnv);
            }

            // Generate artifacts based on the resolved bindings
            generateArtifacts(model, grpcBinding, restBinding, descriptorSet);
        }

        if (generateOrchestrator) {
            boolean generateCli = shouldGenerateOrchestratorCli(orchestratorElements);
            generateOrchestratorServer(descriptorSet, generateCli, orchestratorElements);
            if (pipelineStepElements.isEmpty()) {
                generateOrchestratorClientsFromTemplate(descriptorSet);
            }
        }

        return true;
    }

    /**
     * Generate Java source artifacts for the given pipeline step model and record their roles.
     * <p>
     * For each target enabled on the model this method creates a source file, delegates rendering
     * to the appropriate renderer (gRPC service, client step or REST resource) and records the
     * generated class with the corresponding role in the role metadata generator.
     *
     * @param model       the pipeline step model containing service/package names and enabled targets
     * @param grpcBinding gRPC binding information used for gRPC and client generation; may be null if not applicable
     * @param restBinding REST binding information used for REST resource generation; may be null if not applicable
     */
    private void generateArtifacts(PipelineStepModel model, GrpcBinding grpcBinding, RestBinding restBinding,
                                   DescriptorProtos.FileDescriptorSet descriptorSet) {
        java.util.Set<String> enabledAspects = pipelineAspects.stream()
            .map(aspect -> aspect.name().toLowerCase())
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
        ClassName cacheKeyGenerator = resolveCacheKeyGenerator();

        for (GenerationTarget target : model.enabledTargets()) {
            try {
                switch (target) {
                    case GRPC_SERVICE:
                        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && !pluginHost) {
                            break;
                        }
                        if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
                            generateSideEffectBean(model, resolveRoleOutputDir(DeploymentRole.PLUGIN_SERVER));
                        }
                        String grpcClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.generatedName() + GRPC_SERVICE_SUFFIX;
                        DeploymentRole grpcRole = model.deploymentRole();
                        grpcRenderer.render(grpcBinding, new GenerationContext(
                            processingEnv,
                            resolveRoleOutputDir(grpcRole),
                            grpcRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                        roleMetadataGenerator.recordClassWithRole(grpcClassName, grpcRole.name());
                        break;
                    case CLIENT_STEP:
                        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && pluginHost) {
                            break;
                        }
                        String clientClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.generatedName().replace("Service", "") +
                                GRPC_CLIENT_STEP_SUFFIX;
                        DeploymentRole clientRole = resolveClientRole(model.deploymentRole());
                        clientRenderer.render(grpcBinding, new GenerationContext(
                            processingEnv,
                            resolveRoleOutputDir(clientRole),
                            clientRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                        roleMetadataGenerator.recordClassWithRole(clientClassName, clientRole.name());
                        break;
                    case REST_RESOURCE:
                        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && !pluginHost) {
                            break;
                        }
                        if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
                            generateSideEffectBean(model, resolveRoleOutputDir(DeploymentRole.REST_SERVER));
                        }
                        if (restBinding == null) {
                            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                                "Skipping REST resource generation for '" + model.generatedName() +
                                    "' because no REST binding is available.");
                            break;
                        }
                        String restClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.generatedName().replace("Service", "").replace("Reactive", "") +
                                REST_RESOURCE_SUFFIX;
                        DeploymentRole restRole = DeploymentRole.REST_SERVER;
                        restRenderer.render(restBinding, new GenerationContext(
                            processingEnv,
                            resolveRoleOutputDir(restRole),
                            restRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                        roleMetadataGenerator.recordClassWithRole(restClassName, restRole.name());
                        break;
                    case REST_CLIENT_STEP:
                        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && pluginHost) {
                            break;
                        }
                        if (restBinding == null) {
                            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                                "Skipping REST client step generation for '" + model.generatedName() +
                                    "' because no REST binding is available.");
                            break;
                        }
                        String restClientClassName = model.servicePackage() + PIPELINE_PACKAGE_SUFFIX +
                                "." + model.generatedName().replace("Service", "") +
                                REST_CLIENT_STEP_SUFFIX;
                        DeploymentRole restClientRole = resolveClientRole(model.deploymentRole());
                        restClientRenderer.render(restBinding, new GenerationContext(
                            processingEnv,
                            resolveRoleOutputDir(restClientRole),
                            restClientRole,
                            enabledAspects,
                            cacheKeyGenerator,
                            descriptorSet));
                        roleMetadataGenerator.recordClassWithRole(restClientClassName, restClientRole.name());
                        break;
                }
            } catch (IOException e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "Failed to generate " + target + " for " + model.serviceName() + ": " + e.getMessage());
            }
        }
    }

    private Path resolveGeneratedSourcesRoot(ProcessingEnvironment processingEnv) {
        String configured = processingEnv.getOptions().get("pipeline.generatedSourcesDir");
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured);
        }

        String fallback = processingEnv.getOptions().get("pipeline.generatedSourcesRoot");
        if (fallback != null && !fallback.isBlank()) {
            return Paths.get(fallback);
        }

        return Paths.get(System.getProperty("user.dir"), "target", "generated-sources", "pipeline");
    }

    private ClassName resolveCacheKeyGenerator() {
        String configured = processingEnv.getOptions().get("pipeline.cache.keyGenerator");
        if (configured == null || configured.isBlank()) {
            return null;
        }
        return ClassName.bestGuess(configured);
    }

    private Path resolveRoleOutputDir(DeploymentRole role) throws IOException {
        String directoryName = role.name().toLowerCase().replace('_', '-');
        Path roleDir = generatedSourcesRoot.resolve(directoryName);
        Files.createDirectories(roleDir);
        return roleDir;
    }

    private void generateSideEffectBean(PipelineStepModel model, Path outputDir) throws IOException {
        TypeName outputDomainType = model.outboundDomainType();
        if (outputDomainType == null) {
            return;
        }

        String key = model.serviceClassName() + ":" + outputDomainType;
        if (!generatedSideEffectBeanKeys.add(key)) {
            return;
        }

        ClassName pluginClass = model.serviceClassName();
        String outputSimpleName = outputDomainType instanceof ClassName className
            ? className.simpleName()
            : "Output";
        String beanClassName = pluginClass.simpleName() + outputSimpleName + "SideEffectBean";

        TypeName parameterizedPluginType = ParameterizedTypeName.get(pluginClass, outputDomainType);

        TypeSpec beanClass = TypeSpec.classBuilder(beanClassName)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
            .superclass(parameterizedPluginType)
            .build();

        JavaFile javaFile = JavaFile.builder(
                model.servicePackage() + PIPELINE_PACKAGE_SUFFIX,
                beanClass)
            .build();
        javaFile.writeTo(outputDir);
    }

    private DeploymentRole resolveClientRole(DeploymentRole serverRole) {
        return switch (serverRole) {
            case PLUGIN_SERVER -> DeploymentRole.PLUGIN_CLIENT;
            case PIPELINE_SERVER -> DeploymentRole.ORCHESTRATOR_CLIENT;
            case ORCHESTRATOR_CLIENT, PLUGIN_CLIENT, REST_SERVER -> serverRole;
        };
    }

    private PipelineStepModel applyTransportTargets(PipelineStepModel model, boolean allowClientTargets) {
        java.util.Set<GenerationTarget> targets = resolveTransportTargets(allowClientTargets);

        return new PipelineStepModel(
            model.serviceName(),
            model.generatedName(),
            model.servicePackage(),
            model.serviceClassName(),
            model.inputMapping(),
            model.outputMapping(),
            model.streamingShape(),
            targets,
            model.executionMode(),
            model.deploymentRole(),
            model.sideEffect(),
            model.cacheKeyGenerator());
    }

    private java.util.Set<GenerationTarget> resolveTransportTargets(boolean allowClientTargets) {
        if (transportMode == TransportMode.REST) {
            return allowClientTargets
                ? java.util.Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.REST_CLIENT_STEP)
                : java.util.Set.of(GenerationTarget.REST_RESOURCE);
        }
        return allowClientTargets
            ? java.util.Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP)
            : java.util.Set.of(GenerationTarget.GRPC_SERVICE);
    }

    private GrpcBinding rebuildGrpcBinding(GrpcBinding binding, PipelineStepModel model) {
        if (binding == null) {
            return null;
        }
        return new GrpcBinding(model, binding.serviceDescriptor(), binding.methodDescriptor());
    }

    private RestBinding rebuildRestBinding(RestBinding binding, PipelineStepModel model) {
        if (binding == null) {
            return null;
        }
        return new RestBinding(model, binding.restPathOverride());
    }

    private java.util.List<org.pipelineframework.processor.ir.PipelineAspectModel> loadPipelineAspects(ProcessingEnvironment processingEnv) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = resolveModuleDir();
        java.util.Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return java.util.List.of();
        }

        PipelineAspectConfigLoader loader = new PipelineAspectConfigLoader();
        try {
            return loader.load(configPath.get());
        } catch (Exception e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                "Failed to load pipeline aspects from " + configPath.get() + ": " + e.getMessage());
            throw e;
        }
    }

    private PipelineTemplateConfig loadPipelineTemplateConfig(ProcessingEnvironment processingEnv) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = resolveModuleDir();
        java.util.Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return null;
        }

        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();
        try {
            return loader.load(configPath.get());
        } catch (Exception e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Failed to load pipeline template config from " + configPath.get() + ": " + e.getMessage());
            return null;
        }
    }

    private TransportMode loadPipelineTransport(ProcessingEnvironment processingEnv) {
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = resolveModuleDir();
        java.util.Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            return TransportMode.GRPC;
        }

        PipelineStepConfigLoader stepLoader = new PipelineStepConfigLoader();
        try {
            PipelineStepConfigLoader.StepConfig stepConfig = stepLoader.load(configPath.get());
            String transport = stepConfig.transport();
            if (transport == null || transport.isBlank()) {
                return TransportMode.GRPC;
            }
            if ("REST".equalsIgnoreCase(transport)) {
                return TransportMode.REST;
            }
            if (!"GRPC".equalsIgnoreCase(transport)) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                    "Unknown pipeline transport '" + transport + "'; defaulting to GRPC.");
            }
        } catch (Exception e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Failed to load pipeline transport from " + configPath.get() + ": " + e.getMessage());
        }
        return TransportMode.GRPC;
    }

    private boolean shouldGenerateOrchestrator(java.util.Set<? extends Element> orchestratorElements) {
        if (orchestratorElements != null && !orchestratorElements.isEmpty()) {
            return true;
        }
        String option = processingEnv.getOptions().get("pipeline.orchestrator.generate");
        if (option == null || option.isBlank()) {
            return false;
        }
        String normalized = option.trim();
        return "true".equalsIgnoreCase(normalized) || "1".equals(normalized);
    }

    private void generateOrchestratorServer(DescriptorProtos.FileDescriptorSet descriptorSet,
                                            boolean generateCli,
                                            java.util.Set<? extends Element> orchestratorElements) {
        if (orchestratorGenerated) {
            return;
        }
        orchestratorGenerated = true;

        OrchestratorBinding binding = buildOrchestratorBinding(pipelineTemplateConfig, orchestratorElements);
        if (binding == null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Skipping orchestrator generation because pipeline template config is missing or incomplete.");
            return;
        }

        try {
            String transport = binding.normalizedTransport();
            if ("REST".equalsIgnoreCase(transport)) {
                DeploymentRole role = DeploymentRole.REST_SERVER;
                orchestratorRestRenderer.render(binding, new GenerationContext(
                    processingEnv,
                    resolveRoleOutputDir(role),
                    role,
                    java.util.Set.of(),
                    null,
                    descriptorSet));
            } else {
                DeploymentRole role = DeploymentRole.PIPELINE_SERVER;
                orchestratorGrpcRenderer.render(binding, new GenerationContext(
                    processingEnv,
                    resolveRoleOutputDir(role),
                    role,
                    java.util.Set.of(),
                    null,
                    descriptorSet));
            }

            if (generateCli) {
                DeploymentRole role = DeploymentRole.ORCHESTRATOR_CLIENT;
                orchestratorCliRenderer.render(binding, new GenerationContext(
                    processingEnv,
                    resolveRoleOutputDir(role),
                    role,
                    java.util.Set.of(),
                    null,
                    descriptorSet));
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                "Failed to generate orchestrator server: " + e.getMessage());
        }
    }

    private boolean shouldGenerateOrchestratorCli(java.util.Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return false;
        }
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = element.getAnnotation(PipelineOrchestrator.class);
            if (annotation == null) {
                continue;
            }
            if (annotation.generateCli()) {
                return true;
            }
        }
        return false;
    }

    private OrchestratorBinding buildOrchestratorBinding(PipelineTemplateConfig config,
                                                         java.util.Set<? extends Element> orchestratorElements) {
        if (config == null) {
            return null;
        }
        java.util.List<PipelineTemplateStep> steps = config.steps();
        if (steps == null || steps.isEmpty()) {
            return null;
        }
        String basePackage = config.basePackage();
        if (basePackage == null || basePackage.isBlank()) {
            return null;
        }
        PipelineTemplateStep first = steps.getFirst();
        PipelineTemplateStep last = steps.getLast();
        if (first == null || last == null) {
            return null;
        }
        String inputType = first.inputTypeName();
        String outputType = last.outputTypeName();
        if (inputType == null || inputType.isBlank() || outputType == null || outputType.isBlank()) {
            return null;
        }

        boolean inputStreaming = isStreamingInputCardinality(first.cardinality());
        boolean outputStreaming = inputStreaming;
        for (PipelineTemplateStep step : steps) {
            outputStreaming = applyCardinalityToStreaming(step.cardinality(), outputStreaming);
        }

        String firstServiceNameFormatted = formatForClassName(stripProcessPrefix(first.name()));
        String firstServiceName = "Process" + firstServiceNameFormatted + "Service";
        StreamingShape firstStreamingShape = streamingShape(first.cardinality());

        PipelineStepModel model = new PipelineStepModel(
            ORCHESTRATOR_SERVICE,
            ORCHESTRATOR_SERVICE,
            basePackage + ".orchestrator.service",
            ClassName.get(basePackage + ".orchestrator.service", ORCHESTRATOR_SERVICE),
            null,
            null,
            streamingShape(inputStreaming, outputStreaming),
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            ExecutionMode.DEFAULT,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );

        PipelineOrchestrator orchestratorAnnotation = resolveOrchestratorAnnotation(orchestratorElements);
        String cliName = orchestratorAnnotation == null ? null : emptyToNull(orchestratorAnnotation.name());
        String cliDescription = orchestratorAnnotation == null ? null : emptyToNull(orchestratorAnnotation.description());
        String cliVersion = orchestratorAnnotation == null ? null : emptyToNull(orchestratorAnnotation.version());

        return new OrchestratorBinding(
            model,
            basePackage,
            config.transport(),
            inputType,
            outputType,
            inputStreaming,
            outputStreaming,
            firstServiceName,
            firstStreamingShape,
            cliName,
            cliDescription,
            cliVersion
        );
    }

    private void generateOrchestratorClientsFromTemplate(DescriptorProtos.FileDescriptorSet descriptorSet) {
        if (pipelineTemplateConfig == null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Skipping orchestrator client generation because pipeline template config is missing.");
            return;
        }
        java.util.List<PipelineTemplateStep> steps = pipelineTemplateConfig.steps();
        if (steps == null || steps.isEmpty()) {
            return;
        }
        String basePackage = pipelineTemplateConfig.basePackage();
        if (basePackage == null || basePackage.isBlank()) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Skipping orchestrator client generation because basePackage is missing.");
            return;
        }

        java.util.List<PipelineStepModel> clientModels = buildClientModelsFromTemplate(steps, basePackage);
        for (PipelineStepModel model : clientModels) {
            GrpcBinding grpcBinding = null;
            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP)) {
                grpcBinding = bindingResolver.resolve(model, descriptorSet);
            }
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, processingEnv);
            }
            try {
                generateArtifacts(model, grpcBinding, restBinding, descriptorSet);
            } catch (Exception e) {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                    "Failed to generate orchestrator client for '" + model.serviceName() + "': " + e.getMessage());
            }
        }
    }

    private java.util.List<PipelineStepModel> buildClientModelsFromTemplate(
        java.util.List<PipelineTemplateStep> steps,
        String basePackage
    ) {
        java.util.List<PipelineStepModel> models = new java.util.ArrayList<>();
        PipelineTemplateStep previous = null;
        for (int i = 0; i < steps.size(); i++) {
            PipelineTemplateStep step = steps.get(i);
            if (step == null || step.name() == null || step.name().isBlank()) {
                continue;
            }
            String serviceNameFormatted = formatForClassName(stripProcessPrefix(step.name()));
            String serviceName = "Process" + serviceNameFormatted + "Service";
            String serviceNameForPackage = toServiceName(step.name())
                .replace("-svc", "")
                .replace("-", "_");
            String servicePackage = basePackage + "." + serviceNameForPackage + ".service";
            ClassName serviceClassName = ClassName.get(servicePackage, serviceName);

            String inputTypeName = step.inputTypeName();
            if (i > 0 && previous != null) {
                inputTypeName = previous.outputTypeName();
            }

            TypeMapping inputMapping = new TypeMapping(
                ClassName.get(basePackage + ".common.domain", inputTypeName),
                ClassName.get(basePackage + ".common.mapper", inputTypeName + "Mapper"),
                true);
            TypeMapping outputMapping = new TypeMapping(
                ClassName.get(basePackage + ".common.domain", step.outputTypeName()),
                ClassName.get(basePackage + ".common.mapper", step.outputTypeName() + "Mapper"),
                true);

            StreamingShape shape = streamingShape(step.cardinality());
            java.util.Set<GenerationTarget> targets = transportMode == TransportMode.REST
                ? java.util.Set.of(GenerationTarget.REST_CLIENT_STEP)
                : java.util.Set.of(GenerationTarget.CLIENT_STEP);

            PipelineStepModel model = new PipelineStepModel(
                serviceName,
                serviceName,
                servicePackage,
                serviceClassName,
                inputMapping,
                outputMapping,
                shape,
                targets,
                ExecutionMode.DEFAULT,
                DeploymentRole.ORCHESTRATOR_CLIENT,
                false,
                null
            );
            models.add(model);
            previous = step;
        }
        return models;
    }

    private StreamingShape streamingShape(String cardinality) {
        if ("EXPANSION".equalsIgnoreCase(cardinality)) {
            return StreamingShape.UNARY_STREAMING;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return StreamingShape.STREAMING_UNARY;
        }
        if ("MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return StreamingShape.STREAMING_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }

    private String toServiceName(String name) {
        if (name == null || name.isBlank()) {
            return "";
        }
        String replaced = name.replaceAll("[^A-Za-z0-9]", "-").toLowerCase(java.util.Locale.ROOT);
        String collapsed = replaced.replaceAll("-+", "-");
        if (collapsed.startsWith("-")) {
            collapsed = collapsed.substring(1);
        }
        if (collapsed.endsWith("-")) {
            collapsed = collapsed.substring(0, collapsed.length() - 1);
        }
        return collapsed + "-svc";
    }

    private String formatForClassName(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        String[] parts = input.split(" ");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            builder.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                builder.append(lower.substring(1));
            }
        }
        return builder.toString();
    }

    private String stripProcessPrefix(String name) {
        if (name == null) {
            return "";
        }
        if (name.startsWith("Process ")) {
            return name.substring("Process ".length());
        }
        return name;
    }

    private PipelineOrchestrator resolveOrchestratorAnnotation(java.util.Set<? extends Element> orchestratorElements) {
        if (orchestratorElements == null || orchestratorElements.isEmpty()) {
            return null;
        }
        for (Element element : orchestratorElements) {
            PipelineOrchestrator annotation = element.getAnnotation(PipelineOrchestrator.class);
            if (annotation != null) {
                return annotation;
            }
        }
        return null;
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private StreamingShape streamingShape(boolean inputStreaming, boolean outputStreaming) {
        if (inputStreaming && outputStreaming) {
            return StreamingShape.STREAMING_STREAMING;
        }
        if (inputStreaming) {
            return StreamingShape.STREAMING_UNARY;
        }
        if (outputStreaming) {
            return StreamingShape.UNARY_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }

    private boolean isStreamingInputCardinality(String cardinality) {
        return "REDUCTION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality);
    }

    private boolean applyCardinalityToStreaming(String cardinality, boolean currentStreaming) {
        if ("EXPANSION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return true;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return false;
        }
        return currentStreaming;
    }

    private java.util.List<ResolvedStep> buildPluginHostSteps(
            java.util.List<PipelineAspectModel> aspectsForExpansion,
            java.util.Set<String> pluginAspectNames) {
        if (aspectsForExpansion.isEmpty()) {
            return java.util.List.of();
        }
        if (pluginAspectNames == null || pluginAspectNames.isEmpty()) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "No @PipelinePlugin aspect name found; skipping plugin-server generation.");
            return java.util.List.of();
        }

        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Path moduleDir = resolveModuleDir();
        java.util.Optional<Path> configPath = locator.locate(moduleDir);
        if (configPath.isEmpty()) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "No pipeline config found for plugin host; skipping plugin-server generation.");
            return java.util.List.of();
        }

        PipelineStepConfigLoader stepLoader = new PipelineStepConfigLoader();
        PipelineStepConfigLoader.StepConfig stepConfig = stepLoader.load(configPath.get());
        if (stepConfig.basePackage().isBlank()) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                "Missing basePackage in pipeline config; skipping plugin-server generation.");
            return java.util.List.of();
        }

        java.util.Set<String> inputTypes = new java.util.LinkedHashSet<>(stepConfig.inputTypes());
        java.util.Set<String> outputTypes = new java.util.LinkedHashSet<>(stepConfig.outputTypes());
        java.util.List<ResolvedStep> syntheticSteps = new java.util.ArrayList<>();
        java.util.Set<String> keys = new java.util.HashSet<>();

        for (org.pipelineframework.processor.ir.PipelineAspectModel aspect : aspectsForExpansion) {
            if (!pluginAspectNames.contains(aspect.name())) {
                continue;
            }
            java.util.Set<String> typeNames = aspect.position() == org.pipelineframework.processor.ir.AspectPosition.BEFORE_STEP
                ? inputTypes
                : outputTypes;
            for (String typeName : typeNames) {
                if (typeName == null || typeName.isBlank()) {
                    continue;
                }
                String key = aspect.name() + ":" + typeName + ":" + aspect.position();
                if (!keys.add(key)) {
                    continue;
                }
                ResolvedStep synthetic = buildPluginHostStep(stepConfig.basePackage(), typeName, aspect);
                if (synthetic != null) {
                    syntheticSteps.add(synthetic);
                }
            }
        }

        return syntheticSteps;
    }

    private boolean isCacheAspect(PipelineAspectModel aspect) {
        return "cache".equalsIgnoreCase(aspect.name());
    }

    private ResolvedStep buildPluginHostStep(
            String basePackage,
            String typeName,
            org.pipelineframework.processor.ir.PipelineAspectModel aspect) {
        Object pluginClassValue = aspect.config().get("pluginImplementationClass");
        if (pluginClassValue == null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                "Aspect '" + aspect.name() + "' must specify pluginImplementationClass in config.");
            return null;
        }

        String pluginImplementationClass = String.valueOf(pluginClassValue);
        ClassName pluginClassName = ClassName.bestGuess(pluginImplementationClass);

        String serviceName = "Observe" + toPascalCase(aspect.name()) + typeName + "SideEffectService";
        String generatedName = toPascalCase(aspect.name()) + typeName + "SideEffect";

        ClassName domainType = ClassName.get(basePackage + ".common.domain", typeName);
        ClassName mapperType = ClassName.get(basePackage + ".common.mapper", typeName + "Mapper");
        TypeMapping mapping = new TypeMapping(domainType, mapperType, true);

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(generatedName)
            .servicePackage(basePackage + ".service")
            .serviceClassName(pluginClassName)
            .inputMapping(mapping)
            .outputMapping(mapping)
            .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
            .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
            .enabledTargets(resolveTransportTargets(false))
            .deploymentRole(DeploymentRole.PLUGIN_SERVER)
            .sideEffect(true)
            .build();

        return new ResolvedStep(model, null, null);
    }

    private String toPascalCase(String input) {
        if (input == null || input.isBlank()) {
            return input;
        }
        StringBuilder builder = new StringBuilder();
        String[] parts = input.split("[^a-zA-Z0-9]+");
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(part.substring(0, 1).toUpperCase());
            if (part.length() > 1) {
                builder.append(part.substring(1));
            }
        }
        return builder.toString();
    }

    private Path resolveModuleDir() {
        if (generatedSourcesRoot != null) {
            Path candidate = generatedSourcesRoot;
            // .../target/generated-sources/pipeline -> module root
            for (int i = 0; i < 3 && candidate != null; i++) {
                candidate = candidate.getParent();
            }
            if (candidate != null) {
                return candidate;
            }
        }
        return Paths.get(System.getProperty("user.dir"));
    }

}
