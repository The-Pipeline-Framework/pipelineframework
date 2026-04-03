package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
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

    /**
     * Deletes and recreates the generated-sources root used for pipeline-rendered Java artifacts.
     *
     * @param ctx compilation context containing the generated sources root
     */
    public void resetGeneratedSourcesRoot(PipelineCompilationContext ctx) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        Path root = ctx.getGeneratedSourcesRoot();
        if (root == null) {
            return;
        }
        try {
            if (Files.exists(root)) {
                try (var stream = Files.walk(root)) {
                    stream.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to delete path: " + path, e);
                            }
                        });
                }
            }
            Files.createDirectories(root);
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                reportResetFailure(ctx, root, ioException);
                throw new IllegalStateException("Failed to reset generated sources root '" + root + "'", ioException);
            }
            throw e;
        } catch (IOException e) {
            reportResetFailure(ctx, root, e);
            throw new IllegalStateException("Failed to reset generated sources root '" + root + "'", e);
        }
    }

    private void reportResetFailure(PipelineCompilationContext ctx, Path root, IOException e) {
        if (ctx.getProcessingEnv() != null && ctx.getProcessingEnv().getMessager() != null) {
            ctx.getProcessingEnv().getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "Failed to reset generated sources root '" + root + "': " + e.getMessage());
        }
    }
}
