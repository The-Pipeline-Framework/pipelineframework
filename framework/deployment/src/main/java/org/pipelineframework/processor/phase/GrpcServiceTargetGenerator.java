package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;
import javax.tools.Diagnostic;

import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;

/**
 * Target generator for gRPC service artifacts.
 */
public class GrpcServiceTargetGenerator implements TargetGenerator {

    private static final String PIPELINE_PACKAGE_SEGMENT = ".pipeline.";

    private final GrpcServiceAdapterRenderer renderer;
    private final GenerationPolicy policy;
    private final GenerationPathResolver pathResolver;
    private final SideEffectBeanService sideEffectBeanService;

    public GrpcServiceTargetGenerator(
            GrpcServiceAdapterRenderer renderer,
            GenerationPolicy policy,
            GenerationPathResolver pathResolver,
            SideEffectBeanService sideEffectBeanService) {
        this.renderer = Objects.requireNonNull(renderer, "renderer must not be null");
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
        this.sideEffectBeanService = Objects.requireNonNull(
            sideEffectBeanService, "sideEffectBeanService must not be null");
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.GRPC_SERVICE;
    }

    @Override
    public void generate(GenerationRequest request) throws IOException {
        var ctx = request.ctx();
        var model = request.model();

        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && !policy.allowPluginServerArtifacts(ctx)) {
            return;
        }

        if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && request.grpcBinding() != null) {
            DeploymentRole sideEffectOutputRole = ctx.isTransportModeLocal()
                ? DeploymentRole.ORCHESTRATOR_CLIENT
                : DeploymentRole.PLUGIN_SERVER;
            boolean shouldGenerate = true;
            if (ctx.isTransportModeLocal()) {
                String key = model.servicePackage() + PIPELINE_PACKAGE_SEGMENT + model.serviceName();
                shouldGenerate = request.generatedSideEffectBeans().add(key);
            }
            if (shouldGenerate) {
                sideEffectBeanService.generateSideEffectBean(
                    ctx,
                    model,
                    DeploymentRole.PLUGIN_SERVER,
                    sideEffectOutputRole,
                    request.grpcBinding());
            }
        }

        if (ctx.isTransportModeLocal()) {
            return;
        }

        if (request.grpcBinding() == null) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "Skipping gRPC service generation for '" + model.generatedName()
                        + "' because no gRPC binding is available.");
            }
            return;
        }

        String servicePackage = model.servicePackage();
        if (servicePackage == null || servicePackage.isBlank()) {
            throw new IllegalArgumentException("PipelineStepModel.servicePackage() must not be null/blank");
        }
        String generatedName = model.generatedName();
        if (generatedName == null || generatedName.isBlank()) {
            throw new IllegalArgumentException("PipelineStepModel.generatedName() must not be null/blank");
        }

        DeploymentRole role = model.deploymentRole();
        renderer.render(request.grpcBinding(), new GenerationContext(
            ctx.getProcessingEnv(),
            pathResolver.resolveRoleOutputDir(ctx, role),
            role,
            request.enabledAspects(),
            request.cacheKeyGenerator(),
            request.descriptorSet()));

        String className = servicePackage + PIPELINE_PACKAGE_SEGMENT + generatedName + "GrpcService";
        request.roleMetadataGenerator().recordClassWithRole(className, role.name());
    }
}
