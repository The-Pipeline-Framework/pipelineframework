package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;
import javax.tools.Diagnostic;

import org.jboss.logging.Logger;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.LocalClientStepRenderer;

/**
 * Target generator for local client step artifacts.
 */
public class LocalClientStepTargetGenerator implements TargetGenerator {

    private static final Logger LOG = Logger.getLogger(LocalClientStepTargetGenerator.class);
    private static final String SERVICE_SUFFIX = "Service";

    private final LocalClientStepRenderer renderer;
    private final GenerationPolicy policy;
    private final GenerationPathResolver pathResolver;
    private final SideEffectBeanService sideEffectBeanService;

    public LocalClientStepTargetGenerator(
            LocalClientStepRenderer renderer,
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
        return GenerationTarget.LOCAL_CLIENT_STEP;
    }

    @Override
    public void generate(GenerationRequest request) throws IOException {
        var ctx = request.ctx();
        var model = request.model();
        String generatedName = model.generatedName();
        if (generatedName == null || generatedName.isBlank()) {
            throw new IllegalArgumentException("PipelineStepModel.generatedName() must not be null/blank");
        }
        String servicePackage = model.servicePackage();
        if (servicePackage == null || servicePackage.isBlank()) {
            throw new IllegalArgumentException("PipelineStepModel.servicePackage() must not be null/blank");
        }

        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
            return;
        }

        if (ctx.getProcessingEnv() == null) {
            LOG.warnf(
                "Skipping local client step generation for '%s' because processing environment is null.",
                model.generatedName());
            return;
        }

        if (model.sideEffect() && model.deploymentRole() == DeploymentRole.PLUGIN_SERVER) {
            String key = model.servicePackage() + ".pipeline." + model.serviceName();
            if (request.grpcBinding() != null && request.generatedSideEffectBeans().add(key)) {
                sideEffectBeanService.generateSideEffectBean(
                    ctx,
                    model,
                    DeploymentRole.PLUGIN_SERVER,
                    DeploymentRole.ORCHESTRATOR_CLIENT,
                    request.grpcBinding());
            }
        }

        if (request.localBinding() == null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                Diagnostic.Kind.WARNING,
                "Skipping local client step generation for '" + model.generatedName()
                    + "' because no local binding is available.");
            return;
        }

        DeploymentRole role = policy.resolveClientRole(model.deploymentRole());
        renderer.render(request.localBinding(), new GenerationContext(
            ctx.getProcessingEnv(),
            pathResolver.resolveRoleOutputDir(ctx, role),
            role,
            request.enabledAspects(),
            request.cacheKeyGenerator(),
            request.descriptorSet()));

        if (generatedName.endsWith(SERVICE_SUFFIX)) {
            generatedName = generatedName.substring(0, generatedName.length() - SERVICE_SUFFIX.length());
        }
        String className = servicePackage + ".pipeline." + generatedName + "LocalClientStep";
        request.roleMetadataGenerator().recordClassWithRole(className, role.name());
    }
}
