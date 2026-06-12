package org.pipelineframework.orchestrator;

import java.nio.file.Path;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;

public interface PipelineReleaseArtifactStore {

    PipelineReleaseStoredArtifact store(Path sourcePath);

    void verify(PipelineReleaseRecord record);
}
