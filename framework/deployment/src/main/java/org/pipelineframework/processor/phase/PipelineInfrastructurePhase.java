package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;

/**
 * Resolves filesystem paths and populates the compilation context with infrastructure information.
 * This phase handles all filesystem path resolution without any semantic logic.
 */
public class PipelineInfrastructurePhase implements PipelineCompilationPhase {

    /**
     * Creates a new PipelineInfrastructurePhase.
     */
    public PipelineInfrastructurePhase() {
    }

    @Override
    public String name() {
        return "Pipeline Infrastructure Phase";
    }

    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Resolve generated sources root
        Path generatedSourcesRoot = resolveGeneratedSourcesRoot(ctx);
        ctx.setGeneratedSourcesRoot(generatedSourcesRoot);
        
        // Resolve module directory
        Path moduleDir = resolveModuleDir(ctx, generatedSourcesRoot);
        ctx.setModuleDir(moduleDir);
        
        // Create role output directories
        createRoleOutputDirectories(ctx);
    }

    /**
     * Resolves the generated sources root directory from processing environment options.
     * 
     * @param ctx the compilation context
     * @return the path to the generated sources root directory
     */
    private Path resolveGeneratedSourcesRoot(PipelineCompilationContext ctx) {
        java.util.Map<String, String> options = ctx.getProcessingEnv().getOptions();
        String configured = options.get("pipeline.generatedSourcesDir");
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured);
        }

        String fallback = options.get("pipeline.generatedSourcesRoot");
        if (fallback != null && !fallback.isBlank()) {
            return Path.of(fallback);
        }

        return Path.of(System.getProperty("user.dir"), "target", "generated-sources", "pipeline");
    }

    /**
     * Resolves the module directory from the generated sources root.
     * 
     * @param ctx the compilation context
     * @param generatedSourcesRoot the generated sources root path
     * @return the path to the module directory
     */
    private Path resolveModuleDir(PipelineCompilationContext ctx, Path generatedSourcesRoot) {
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
        return Path.of(System.getProperty("user.dir"));
    }

    /**
     * Creates the role-specific output directories.
     * 
     * @param ctx the compilation context
     * @throws IOException if directory creation fails
     */
    private void createRoleOutputDirectories(PipelineCompilationContext ctx) throws IOException {
        // This would iterate through all the roles that will be needed and create directories
        // For now, we'll create a basic set of directories based on known roles
        java.util.Set<String> roles = java.util.Set.of(
            "PIPELINE_SERVER", 
            "PLUGIN_SERVER", 
            "ORCHESTRATOR_CLIENT", 
            "PLUGIN_CLIENT", 
            "REST_SERVER"
        );
        
        for (String roleName : roles) {
            String directoryName = roleName.toLowerCase().replace('_', '-');
            Path roleDir = ctx.getGeneratedSourcesRoot().resolve(directoryName);
            Files.createDirectories(roleDir);
        }
    }
}
