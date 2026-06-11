package org.pipelineframework.orchestrator;

import java.nio.file.Path;

/**
 * Stores and verifies coordinator-managed pipeline bundle artifacts.
 */
public interface PipelineBundleArtifactStore {

    /**
     * Copies the validated source artifact into the coordinator-owned store.
     *
     * @param sourcePath validated source JAR path
     * @param manifest parsed bundle manifest
     * @return managed artifact metadata
     */
    PipelineBundleStoredArtifact store(Path sourcePath, PipelineBundleManifest manifest);

    /**
     * Verifies that the stored artifact still matches the registered metadata.
     *
     * @param record bundle registry record
     */
    void verify(PipelineBundleRecord record);
}
