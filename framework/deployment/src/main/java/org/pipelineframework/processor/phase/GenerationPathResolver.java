package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import javax.tools.Diagnostic;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;

/**
 * Resolves generation output paths for deployment roles.
 */
public class GenerationPathResolver {

    /**
     * Resolve and ensure existence of the output directory for the given deployment role.
     *
     * @param ctx compilation context containing generated sources root and messager
     * @param role deployment role to resolve
     * @return resolved output directory, or root when role is null, or null when root is null
     */
    public Path resolveRoleOutputDir(PipelineCompilationContext ctx, DeploymentRole role) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        Path root = ctx.getGeneratedSourcesRoot();
        if (root == null || role == null) {
            return root;
        }
        String dirName = role.name().toLowerCase(Locale.ROOT).replace('_', '-');
        Path outputDir = root.resolve(dirName);
        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
                ctx.getProcessingEnv().getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to create output directory '" + outputDir + "': " + e.getMessage());
            }
            throw new IllegalStateException("Failed to create output directory '" + outputDir + "'", e);
        }
        return outputDir;
    }
}
