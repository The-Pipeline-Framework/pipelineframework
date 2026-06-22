package org.pipelineframework.orchestrator.release;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Loads generated pipeline contract metadata from classpath resources.
 */
@ApplicationScoped
public class PipelineContractDescriptorLoader {

    public Optional<PipelineContractDescriptor> load() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = PipelineContractDescriptorLoader.class.getClassLoader();
        }
        return load(loader);
    }

    public Optional<PipelineContractDescriptor> load(ClassLoader classLoader) {
        if (classLoader == null) {
            return Optional.empty();
        }
        try (InputStream stream = classLoader.getResourceAsStream(PipelineContractDescriptor.RESOURCE_PATH)) {
            if (stream == null) {
                return Optional.empty();
            }
            return Optional.of(load(stream));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + PipelineContractDescriptor.RESOURCE_PATH, e);
        }
    }

    public PipelineContractDescriptor load(InputStream stream) {
        if (stream == null) {
            throw new IllegalArgumentException("Pipeline contract stream must not be null");
        }
        try {
            return PipelineJson.mapper().readValue(stream, PipelineContractDescriptor.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                "Invalid " + PipelineContractDescriptor.RESOURCE_PATH + " JSON: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException(
                "Invalid " + PipelineContractDescriptor.RESOURCE_PATH + ": " + e.getMessage(), e);
        }
    }
}
