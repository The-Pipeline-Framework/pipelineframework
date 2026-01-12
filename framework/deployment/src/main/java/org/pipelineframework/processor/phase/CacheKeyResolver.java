package org.pipelineframework.processor.phase;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineCompilationContext;

/**
 * Resolves cache key generators from processing environment options.
 */
class CacheKeyResolver {

    /**
     * Resolves the cache key generator from processing environment options.
     *
     * @param ctx the compilation context
     * @return the cache key generator class name or null
     */
    static ClassName resolveCacheKeyGenerator(PipelineCompilationContext ctx) {
        String configured = ctx.getProcessingEnv().getOptions().get("pipeline.cache.keyGenerator");
        if (configured == null || configured.isBlank()) {
            return null;
        }
        return ClassName.bestGuess(configured);
    }
}