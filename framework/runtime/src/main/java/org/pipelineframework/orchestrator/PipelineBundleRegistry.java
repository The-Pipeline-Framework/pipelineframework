package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Registry for local/dev hosted bundle metadata.
 */
public interface PipelineBundleRegistry {

    Uni<PipelineBundleRecord> register(PipelineBundleRecord record);

    Uni<List<PipelineBundleRecord>> list(String tenantId, String pipelineId);

    Uni<Optional<PipelineBundleRecord>> get(String tenantId, String pipelineId, String bundleVersionId);

    Uni<Optional<PipelineBundleRecord>> active(String tenantId, String pipelineId);

    Uni<Optional<PipelineBundleRecord>> activate(String tenantId, String pipelineId, String bundleVersionId, long nowEpochMs);
}
