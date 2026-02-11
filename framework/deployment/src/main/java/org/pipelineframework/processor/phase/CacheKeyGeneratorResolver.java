package org.pipelineframework.processor.phase;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineCompilationContext;

/**
 * Resolves cache key generator configuration from processor options.
 */
public class CacheKeyGeneratorResolver {

    private CacheKeyGeneratorResolver() {
    }

    /**
     * Resolve cache key generator class from processing options.
     *
     * @param ctx compilation context
     * @return configured class name, or null when unset
     */
    public static ClassName resolve(PipelineCompilationContext ctx) {
        String configured = ctx.getProcessingEnv().getOptions().get("pipeline.cache.keyGenerator");
        if (configured != null) {
            configured = configured.trim();
        }
        if (configured == null || configured.isBlank()) {
            return null;
        }
        try {
            return ClassName.bestGuess(configured);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid value for option 'pipeline.cache.keyGenerator': '" + configured + "'",
                e);
        }
    }
}
