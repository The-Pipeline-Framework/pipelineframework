package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;
import javax.tools.Diagnostic;

import org.jboss.logging.Logger;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.RestClientStepRenderer;

/**
 * Target generator for REST client step artifacts.
 */
public class RestClientStepTargetGenerator implements TargetGenerator {

    private static final Logger LOG = Logger.getLogger(RestClientStepTargetGenerator.class);
    private static final String SERVICE_SUFFIX = "Service";

    private final RestClientStepRenderer renderer;
    private final GenerationPolicy policy;
    private final GenerationPathResolver pathResolver;

    public RestClientStepTargetGenerator(
            RestClientStepRenderer renderer,
            GenerationPolicy policy,
            GenerationPathResolver pathResolver) {
        this.renderer = Objects.requireNonNull(renderer, "renderer must not be null");
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
    }

    @Override
    public GenerationTarget target() {
        return GenerationTarget.REST_CLIENT_STEP;
    }

    @Override
    public void generate(GenerationRequest request) throws IOException {
        var ctx = request.ctx();
        var model = request.model();

        if (model.deploymentRole() == DeploymentRole.PLUGIN_SERVER && ctx.isPluginHost()) {
            return;
        }

        if (request.restBinding() == null) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "Skipping REST client step generation for '" + model.generatedName()
                        + "' because no REST binding is available.");
            } else {
                LOG.warnf(
                    "Skipping REST client step generation for '%s' because no REST binding is available.",
                    model.generatedName());
            }
            return;
        }

        DeploymentRole role = policy.resolveClientRole(model.deploymentRole());
        renderer.render(request.restBinding(), new GenerationContext(
            ctx.getProcessingEnv(),
            pathResolver.resolveRoleOutputDir(ctx, role),
            role,
            request.enabledAspects(),
            request.cacheKeyGenerator(),
            request.descriptorSet()));

        String generatedName = model.generatedName();
        if (generatedName.endsWith(SERVICE_SUFFIX)) {
            generatedName = generatedName.substring(0, generatedName.length() - SERVICE_SUFFIX.length());
        }
        String className = model.servicePackage() + ".pipeline." + generatedName + "RestClientStep";
        request.roleMetadataGenerator().recordClassWithRole(className, role.name());
    }
}
