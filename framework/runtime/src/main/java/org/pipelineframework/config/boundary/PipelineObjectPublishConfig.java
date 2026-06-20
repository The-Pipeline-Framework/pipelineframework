package org.pipelineframework.config.boundary;

import java.util.Map;

/**
 * Named object publish target declared under top-level {@code publish}.
 *
 * @param name target name
 * @param kind target kind, v1 supports object
 * @param provider object target provider, for example filesystem or s3
 * @param location provider-specific location map
 * @param naming object key naming settings
 * @param payload object payload settings
 */
public record PipelineObjectPublishConfig(
    String name,
    String kind,
    String provider,
    Map<String, Object> location,
    PipelineObjectNamingConfig naming,
    PipelineObjectPublishPayloadConfig payload
) {
    public PipelineObjectPublishConfig {
        name = normalize(name);
        kind = normalize(kind);
        provider = normalize(provider);
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
}
