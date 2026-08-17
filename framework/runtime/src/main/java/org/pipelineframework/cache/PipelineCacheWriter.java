package org.pipelineframework.cache;

import java.time.Duration;

import io.smallrye.mutiny.Uni;

/**
 * SPI for the cache subsystem to store a pipeline cache value with an explicit bounded TTL.
 */
public interface PipelineCacheWriter {
    Uni<Void> put(String key, Object value, Duration ttl);
}
