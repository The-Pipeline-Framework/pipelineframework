package org.pipelineframework.orchestrator.release;

import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Loads pipeline release descriptors from local files or streams.
 */
@ApplicationScoped
public class PipelineReleaseDescriptorLoader {

    public PipelineReleaseDescriptor load(Path path) {
        if (path == null) {
            throw new IllegalArgumentException("releaseDescriptorPath is required");
        }
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("releaseDescriptorPath must point to a readable file");
        }
        try (InputStream stream = Files.newInputStream(path)) {
            return load(stream);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read release descriptor: " + e.getMessage(), e);
        }
    }

    public PipelineReleaseDescriptor load(InputStream stream) {
        if (stream == null) {
            throw new IllegalArgumentException("Pipeline release descriptor stream must not be null");
        }
        try {
            return PipelineJson.mapper().readValue(stream, PipelineReleaseDescriptor.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Invalid pipeline release descriptor JSON: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException("Invalid pipeline release descriptor: " + e.getMessage(), e);
        }
    }
}
