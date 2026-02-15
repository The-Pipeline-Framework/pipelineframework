package org.pipelineframework.processor.phase;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

/**
 * Resolves filesystem paths used during the pipeline discovery phase.
 * Stateless collaborator extracted from PipelineDiscoveryPhase.
 */
class DiscoveryPathResolver {

    private static final int GENERATED_SOURCES_DEPTH = 3; // Maps to "target/generated-sources/pipeline" depth
    private static final Path DEFAULT_GENERATED_SOURCES_ROOT = 
        Paths.get(System.getProperty("user.dir"), "target", "generated-sources", "pipeline");

    /**
     * Resolve the root directory where generated pipeline sources should be written.
     *
     * Checks options for "pipeline.generatedSourcesDir" first, then "pipeline.generatedSourcesRoot",
     * and returns the first non-blank value as a Path. If neither is present, returns
     * {user.dir}/target/generated-sources/pipeline.
     *
     * @param options the processing environment options
     * @return the resolved Path for generated pipeline sources
     */
    Path resolveGeneratedSourcesRoot(Map<String, String> options) {
        if (options == null) {
            return DEFAULT_GENERATED_SOURCES_ROOT;
        }

        String configured = options.get("pipeline.generatedSourcesDir");
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured);
        }

        String fallback = options.get("pipeline.generatedSourcesRoot");
        if (fallback != null && !fallback.isBlank()) {
            return Paths.get(fallback);
        }

        return DEFAULT_GENERATED_SOURCES_ROOT;
    }

    /**
     * Resolve the module root directory by ascending from a generated-sources path
     * or falling back to the current working directory.
     *
     * @param generatedSourcesRoot the path inside the module's generated-sources tree, or null
     * @return the resolved module directory path
     */
    Path resolveModuleDir(Path generatedSourcesRoot) {
        if (generatedSourcesRoot != null) {
            Path candidate = generatedSourcesRoot;
            for (int i = 0; i < GENERATED_SOURCES_DEPTH && candidate != null; i++) {
                candidate = candidate.getParent();
            }
            if (candidate != null) {
                return candidate;
            }
        }
        return Paths.get(System.getProperty("user.dir"));
    }

    /**
     * Resolve the module name from the processing options.
     *
     * @param options the processing environment options
     * @return the trimmed module name, or null if not present or blank
     */
    String resolveModuleName(Map<String, String> options) {
        if (options == null) {
            return null;
        }
        String moduleName = options.get("pipeline.module");
        if (moduleName == null || moduleName.isBlank()) {
            return null;
        }
        return moduleName.trim();
    }
}
