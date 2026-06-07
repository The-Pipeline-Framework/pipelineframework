package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Local/dev registry for release metadata and active release pointers.
 */
public interface PipelineReleaseRegistry {

    Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record);

    Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId);

    Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion);

    Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId);

    Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs);
}
