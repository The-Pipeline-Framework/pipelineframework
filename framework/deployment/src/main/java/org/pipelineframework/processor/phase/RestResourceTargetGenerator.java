package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;
import javax.tools.Diagnostic;

import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
import org.pipelineframework.processor.util.ResourceNameUtils;

/**
 * Target generator for REST resource artifacts.
 */
public class RestResourceTargetGenerator implements TargetGenerator {

    private final RestResourceRenderer renderer;
    private final GenerationPolicy policy;
    private final GenerationPathResolver pathResolver;
    private final SideEffectBeanService sideEffectBeanService;

    public RestResourceTargetGenerator(
            RestResourceRenderer renderer,
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
        return GenerationTarget.REST_RESOURCE;
    }

    @Override
    public void generate(GenerationRequest request) throws IOException {
        var ctx = request.ctx();
        var model = request.model();

        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && !policy.allowPluginServerArtifacts(ctx)) {
            return;
        }

        if (request.restBinding() == null) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "Skipping REST resource generation for '" + model.generatedName()
                        + "' because no REST binding is available.");
            }
            return;
        }
        if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
            sideEffectBeanService.generateSideEffectBean(
                ctx,
                model,
                DeploymentRole.REST_SERVER,
                DeploymentRole.REST_SERVER,
                request.grpcBinding());
        }

        DeploymentRole role = DeploymentRole.REST_SERVER;
        renderer.render(request.restBinding(), new GenerationContext(
            ctx.getProcessingEnv(),
            pathResolver.resolveRoleOutputDir(ctx, role),
            role,
            request.enabledAspects(),
            request.cacheKeyGenerator(),
            request.descriptorSet()));

        String baseName = ResourceNameUtils.normalizeBaseName(model.generatedName());
        String className = model.servicePackage() + ".pipeline." + baseName + "Resource";
        request.roleMetadataGenerator().recordClassWithRole(className, role.name());
    }
}
