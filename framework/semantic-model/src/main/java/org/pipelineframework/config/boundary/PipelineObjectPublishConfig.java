package org.pipelineframework.config.boundary;

import java.util.Map;
import java.util.Optional;

/**
 * Named object publish target declared under top-level {@code publish}.
 *
 * @param name target name
 * @param kind target kind, v1 supports object
 * @param provider object target provider, for example filesystem or s3
 * @param binding optional configured Connector binding that owns published references
 * @param location provider-specific location map
 * @param naming object key naming settings
 * @param payload object payload settings
 * @param grouping object publish grouping settings
 */
public record PipelineObjectPublishConfig(
    String name,
    String kind,
    String provider,
    Optional<String> binding,
    Map<String, Object> location,
    PipelineObjectNamingConfig naming,
    PipelineObjectPublishPayloadConfig payload,
    PipelineObjectPublishGroupingConfig grouping
) {
    public PipelineObjectPublishConfig(
        String name,
        String kind,
        String provider,
        Map<String, Object> location,
        PipelineObjectNamingConfig naming,
        PipelineObjectPublishPayloadConfig payload
    ) {
        this(name, kind, provider, Optional.empty(), location, naming, payload,
            PipelineObjectPublishGroupingConfig.defaults());
    }

    public PipelineObjectPublishConfig(
        String name,
        String kind,
        String provider,
        Map<String, Object> location,
        PipelineObjectNamingConfig naming,
        PipelineObjectPublishPayloadConfig payload,
        PipelineObjectPublishGroupingConfig grouping
    ) {
        this(name, kind, provider, Optional.empty(), location, naming, payload, grouping);
    }

    public PipelineObjectPublishConfig {
        name = normalize(name);
        kind = normalize(kind);
        provider = normalize(provider);
        binding = normalize(binding);
        if (location != null) {
            for (Map.Entry<String, Object> entry : location.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    throw new IllegalArgumentException("publish target location map must not contain null keys or values");
                }
            }
        }
        location = location == null ? Map.of() : Map.copyOf(location);
        naming = naming == null ? PipelineObjectNamingConfig.defaults() : naming;
        payload = payload == null ? PipelineObjectPublishPayloadConfig.defaults() : payload;
        grouping = grouping == null ? PipelineObjectPublishGroupingConfig.defaults() : grouping;
        if (name == null) {
            throw new IllegalArgumentException("publish target name must not be blank");
        }
        if (!"object".equalsIgnoreCase(kind)) {
            throw new IllegalArgumentException("publish target '" + name + "' kind must be object");
        }
        if (provider == null) {
            throw new IllegalArgumentException("publish target '" + name + "' provider must not be blank");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static Optional<String> normalize(Optional<String> value) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String normalized = normalize(value.orElseThrow());
        return normalized == null ? Optional.empty() : Optional.of(normalized);
    }
}
