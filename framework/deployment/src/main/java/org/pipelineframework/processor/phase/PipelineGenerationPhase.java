package org.pipelineframework.processor.phase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.jboss.logging.Logger;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;
import org.pipelineframework.processor.renderer.LocalClientStepRenderer;
import org.pipelineframework.processor.renderer.OrchestratorCliRenderer;
import org.pipelineframework.processor.renderer.OrchestratorGrpcRenderer;
import org.pipelineframework.processor.renderer.OrchestratorIngestClientRenderer;
import org.pipelineframework.processor.renderer.OrchestratorRestResourceRenderer;
import org.pipelineframework.processor.renderer.RestClientStepRenderer;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
import org.pipelineframework.processor.util.OrchestratorClientPropertiesGenerator;
import org.pipelineframework.processor.util.PipelineOrderMetadataGenerator;
import org.pipelineframework.processor.util.PipelineTelemetryMetadataGenerator;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

/**
 * Strategy-driven generation phase for pipeline artifacts.
 */
public class PipelineGenerationPhase implements PipelineCompilationPhase {

    private static final Logger LOG = Logger.getLogger(PipelineGenerationPhase.class);

    private final ProtobufParserService protobufParserService;
    private final OrchestratorGenerationService orchestratorGenerationService;
    private final Map<GenerationTarget, TargetGenerator> targetGenerators;

    public PipelineGenerationPhase() {
        GenerationPathResolver pathResolver = new GenerationPathResolver();
        GenerationPolicy policy = new GenerationPolicy();
        SideEffectBeanService sideEffectBeanService = new SideEffectBeanService(pathResolver);

        this.protobufParserService = new ProtobufParserService(pathResolver);
        this.orchestratorGenerationService = new OrchestratorGenerationService(
            pathResolver,
            new OrchestratorGrpcRenderer(),
            new OrchestratorRestResourceRenderer(),
            new OrchestratorCliRenderer(),
            new OrchestratorIngestClientRenderer());

        Map<GenerationTarget, TargetGenerator> generators = new HashMap<>();
        register(generators, new GrpcServiceTargetGenerator(
            new GrpcServiceAdapterRenderer(GenerationTarget.GRPC_SERVICE),
            policy,
            pathResolver,
            sideEffectBeanService));
        register(generators, new ClientStepTargetGenerator(
            new ClientStepRenderer(GenerationTarget.CLIENT_STEP),
            policy,
            pathResolver));
        register(generators, new LocalClientStepTargetGenerator(
            new LocalClientStepRenderer(),
            policy,
            pathResolver,
            sideEffectBeanService));
        register(generators, new RestResourceTargetGenerator(
            new RestResourceRenderer(),
            policy,
            pathResolver,
            sideEffectBeanService));
        register(generators, new RestClientStepTargetGenerator(
            new RestClientStepRenderer(),
            policy,
            pathResolver));
        this.targetGenerators = Map.copyOf(generators);
    }

    @Override
    public String name() {
        return "Pipeline Generation Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        RoleMetadataGenerator roleMetadataGenerator = new RoleMetadataGenerator(ctx.getProcessingEnv());

        if (ctx.getStepModels().isEmpty() && !ctx.isOrchestratorGenerated()) {
            writeRoleMetadataSafely(ctx, roleMetadataGenerator);
            return;
        }

        Map<String, Object> bindingsMap = ctx.getRendererBindings();
        ClassName cacheKeyGenerator = CacheKeyGeneratorResolver.resolve(ctx);
        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();
        Set<String> generatedSideEffectBeans = new HashSet<>();
        Set<String> enabledAspects = ctx.getAspectModels().stream()
            .map(aspect -> aspect.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());

        for (PipelineStepModel model : ctx.getStepModels()) {
            GenerationRequest request = new GenerationRequest(
                ctx,
                model,
                getBinding(bindingsMap, model.serviceName() + "_grpc", GrpcBinding.class),
                getBinding(bindingsMap, model.serviceName() + "_rest", RestBinding.class),
                getBinding(bindingsMap, model.serviceName() + "_local", LocalBinding.class),
                generatedSideEffectBeans,
                descriptorSet,
                cacheKeyGenerator,
                roleMetadataGenerator,
                enabledAspects);

            for (GenerationTarget target : model.enabledTargets()) {
                TargetGenerator generator = targetGenerators.get(target);
                if (generator == null) {
                    LOG.warnf(
                        "No target generator registered for target '%s' (service=%s, generatedName=%s)",
                        target,
                        model.serviceName(),
                        model.generatedName());
                    continue;
                }
                generator.generate(request);
            }
        }

        if (ctx.isTransportModeGrpc() && descriptorSet != null) {
            protobufParserService.generateProtobufParsers(ctx, descriptorSet);
        }

        if (ctx.isOrchestratorGenerated()) {
            orchestratorGenerationService.generate(ctx, descriptorSet, cacheKeyGenerator);
        }

        writeMetadataSafely(ctx, roleMetadataGenerator);
    }

    private void register(Map<GenerationTarget, TargetGenerator> generators, TargetGenerator generator) {
        generators.put(generator.target(), generator);
    }

    private <T> T getBinding(Map<String, Object> bindingsMap, String key, Class<T> type) {
        Object value = bindingsMap.get(key);
        if (value == null) {
            return null;
        }
        if (!type.isInstance(value)) {
            throw new IllegalStateException(
                "Invalid binding type for key '" + key + "': expected " + type.getSimpleName()
                    + " but found " + value.getClass().getSimpleName());
        }
        return type.cast(value);
    }

    private void writeRoleMetadataSafely(PipelineCompilationContext ctx, RoleMetadataGenerator roleMetadataGenerator) {
        try {
            roleMetadataGenerator.writeRoleMetadata();
        } catch (Exception e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to write role metadata: " + e.getMessage());
            }
        }
    }

    private void writeMetadataSafely(PipelineCompilationContext ctx, RoleMetadataGenerator roleMetadataGenerator) {
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
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    javax.tools.Diagnostic.Kind.WARNING,
                    "Failed to write role metadata: " + e.getMessage());
            }
        }
    }
}
