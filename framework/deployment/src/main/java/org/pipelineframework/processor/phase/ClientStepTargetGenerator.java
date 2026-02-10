package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;

import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.GenerationContext;

/**
 * Target generator for gRPC client step artifacts.
 */
public class ClientStepTargetGenerator implements TargetGenerator {

    private final ClientStepRenderer renderer;
    private final GenerationPolicy policy;
    private final GenerationPathResolver pathResolver;

    public ClientStepTargetGenerator(
            ClientStepRenderer renderer,
            GenerationPolicy policy,
            GenerationPathResolver pathResolver) {
        this.renderer = Objects.requireNonNull(renderer, "renderer must not be null");
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.CLIENT_STEP;
    }

    @Override
    public void generate(GenerationRequest request) throws IOException {
        var ctx = request.ctx();
        var model = request.model();

        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
            return;
        }

        var role = policy.resolveClientRole(model.deploymentRole());
        renderer.render(request.grpcBinding(), new GenerationContext(
            ctx.getProcessingEnv(),
            pathResolver.resolveRoleOutputDir(ctx, role),
            role,
            request.enabledAspects(),
            request.cacheKeyGenerator(),
            request.descriptorSet()));

        String generatedName = model.generatedName();
        if (generatedName == null) {
            throw new IllegalStateException("PipelineStepModel.generatedName() must not be null");
        }
        String servicePackage = model.servicePackage();
        if (servicePackage == null) {
            throw new IllegalStateException("PipelineStepModel.servicePackage() must not be null");
        }
        if (generatedName.endsWith("Service")) {
            generatedName = generatedName.substring(0, generatedName.length() - "Service".length());
        }
        String className = servicePackage + ".pipeline." + generatedName + "GrpcClientStep";
        request.roleMetadataGenerator().recordClassWithRole(className, role.name());
    }
}
