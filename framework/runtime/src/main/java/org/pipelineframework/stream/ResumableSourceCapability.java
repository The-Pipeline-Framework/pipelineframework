package org.pipelineframework.stream;

import io.smallrye.mutiny.Uni;

/**
 * Provider-owned, release-pinned bounded expansion capability.
 *
 * <p>The compiler binds this capability to an internal normal segment continuation. The worker
 * receives only the descriptor, opaque checkpoint, and limit; providers retain parser and source
 * semantics.
 */
public interface ResumableSourceCapability<I, O> {
    ResumableSourceDescriptor descriptor();

    Uni<ResumableSourcePage<O>> readPage(I source, OpaqueSourceCheckpoint checkpoint, int limit);
}
