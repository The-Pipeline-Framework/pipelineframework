package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Loads generated pipeline bundle metadata from classpath resources.
 */
@ApplicationScoped
public class PipelineBundleManifestLoader {

    /**
     * Loads the generated manifest from the thread context class loader.
     *
     * @return manifest when present
     */
    public Optional<PipelineBundleManifest> load() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = PipelineBundleManifestLoader.class.getClassLoader();
        }
        return load(loader);
    }

    /**
     * Loads the generated manifest from the supplied class loader.
     *
     * @param classLoader class loader to inspect
     * @return manifest when present
     */
    public Optional<PipelineBundleManifest> load(ClassLoader classLoader) {
        if (classLoader == null) {
            return Optional.empty();
        }
        try (InputStream stream = classLoader.getResourceAsStream(PipelineBundleManifest.RESOURCE_PATH)) {
            if (stream == null) {
                return Optional.empty();
            }
            return Optional.of(load(stream));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + PipelineBundleManifest.RESOURCE_PATH, e);
        }
    }

    /**
     * Parses one manifest stream.
     *
     * @param stream manifest JSON
     * @return parsed manifest
     */
    public PipelineBundleManifest load(InputStream stream) {
        if (stream == null) {
            throw new IllegalArgumentException("Pipeline bundle manifest stream must not be null");
        }
        try {
            return PipelineJson.mapper().readValue(stream, PipelineBundleManifest.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Invalid " + PipelineBundleManifest.RESOURCE_PATH + " JSON: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException("Invalid " + PipelineBundleManifest.RESOURCE_PATH + ": " + e.getMessage(), e);
        }
    }
}
