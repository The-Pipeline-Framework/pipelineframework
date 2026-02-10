package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import javax.tools.Diagnostic;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.OrchestratorCliRenderer;
import org.pipelineframework.processor.renderer.OrchestratorGrpcRenderer;
import org.pipelineframework.processor.renderer.OrchestratorIngestClientRenderer;
import org.pipelineframework.processor.renderer.OrchestratorRestResourceRenderer;

/**
 * Generates orchestrator artifacts based on orchestrator transport configuration.
 */
public class OrchestratorGenerationService {

    private final GenerationPathResolver pathResolver;
    private final OrchestratorGrpcRenderer grpcRenderer;
    private final OrchestratorRestResourceRenderer restRenderer;
    private final OrchestratorCliRenderer cliRenderer;
    private final OrchestratorIngestClientRenderer ingestClientRenderer;

    public OrchestratorGenerationService(
            GenerationPathResolver pathResolver,
            OrchestratorGrpcRenderer grpcRenderer,
            OrchestratorRestResourceRenderer restRenderer,
            OrchestratorCliRenderer cliRenderer,
            OrchestratorIngestClientRenderer ingestClientRenderer) {
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
        this.grpcRenderer = Objects.requireNonNull(grpcRenderer, "grpcRenderer must not be null");
        this.restRenderer = Objects.requireNonNull(restRenderer, "restRenderer must not be null");
        this.cliRenderer = Objects.requireNonNull(cliRenderer, "cliRenderer must not be null");
        this.ingestClientRenderer = Objects.requireNonNull(
            ingestClientRenderer, "ingestClientRenderer must not be null");
    }

    /**
     * Generate orchestrator artifacts for the active binding.
     */
    public void generate(
            PipelineCompilationContext ctx,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            ClassName cacheKeyGenerator) {
        Object bindingObj = ctx.getRendererBindings().get("orchestrator");
        if (!(bindingObj instanceof OrchestratorBinding binding)) {
            return;
        }

        try {
            String transport = binding.normalizedTransport();
            boolean rest = "REST".equalsIgnoreCase(transport);
            boolean local = "LOCAL".equalsIgnoreCase(transport);

            if (rest) {
                DeploymentRole role = DeploymentRole.REST_SERVER;
                restRenderer.render(binding, createContext(ctx, role, cacheKeyGenerator, descriptorSet));
            } else if (!local) {
                DeploymentRole role = DeploymentRole.PIPELINE_SERVER;
                grpcRenderer.render(binding, createContext(ctx, role, cacheKeyGenerator, descriptorSet));
            }

            if (binding.cliName() != null) {
                DeploymentRole role = DeploymentRole.ORCHESTRATOR_CLIENT;
                cliRenderer.render(binding, createContext(ctx, role, cacheKeyGenerator, descriptorSet));
            }

            if (!rest && !local) {
                DeploymentRole role = DeploymentRole.ORCHESTRATOR_CLIENT;
                ingestClientRenderer.render(binding, createContext(ctx, role, cacheKeyGenerator, descriptorSet));
            }
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate orchestrator artifacts: " + e.getMessage());
            }
        }
    }

    private GenerationContext createContext(
            PipelineCompilationContext ctx,
            DeploymentRole role,
            ClassName cacheKeyGenerator,
            DescriptorProtos.FileDescriptorSet descriptorSet) {
        return new GenerationContext(
            ctx.getProcessingEnv(),
            pathResolver.resolveRoleOutputDir(ctx, role),
            role,
            Set.of(),
            cacheKeyGenerator,
            descriptorSet);
    }
}
