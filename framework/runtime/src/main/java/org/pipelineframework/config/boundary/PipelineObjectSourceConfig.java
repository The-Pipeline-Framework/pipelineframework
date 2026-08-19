package org.pipelineframework.config.boundary;

import java.util.Map;
import java.util.Optional;

/**
 * Named object source configuration declared under top-level {@code sources}.
 *
 * @param name source name
 * @param kind source kind, v1 supports object
 * @param provider object provider name, for example filesystem or s3
 * @param binding optional configured Connector binding that owns emitted references
 * @param location provider-specific location map
 * @param filter object name filter settings
 * @param poll listing poll settings
 * @param identity fields used to derive deterministic object identity
 * @param payload object payload admission settings
 */
public record PipelineObjectSourceConfig(
    String name,
    String kind,
    String provider,
    Optional<String> binding,
    Map<String, Object> location,
    PipelineObjectFilterConfig filter,
    PipelineObjectPollConfig poll,
    PipelineObjectIdentityConfig identity,
    PipelineObjectPayloadConfig payload
) {
    public PipelineObjectSourceConfig(
        String name,
        String kind,
        String provider,
        Map<String, Object> location,
        PipelineObjectFilterConfig filter,
        PipelineObjectPollConfig poll,
        PipelineObjectIdentityConfig identity,
        PipelineObjectPayloadConfig payload
    ) {
        this(name, kind, provider, Optional.empty(), location, filter, poll, identity, payload);
    }

    public PipelineObjectSourceConfig {
        name = normalize(name);
        kind = normalize(kind);
        provider = normalize(provider);
        binding = normalize(binding);
        if (location != null) {
            for (Map.Entry<String, Object> entry : location.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    throw new IllegalArgumentException("source location map must not contain null keys or values");
                }
            }
        }
        location = location == null ? Map.of() : Map.copyOf(location);
        filter = filter == null ? PipelineObjectFilterConfig.defaults() : filter;
        poll = poll == null ? PipelineObjectPollConfig.defaults() : poll;
        identity = identity == null ? PipelineObjectIdentityConfig.defaults() : identity;
        payload = payload == null ? PipelineObjectPayloadConfig.reference() : payload;
        if (name == null) {
            throw new IllegalArgumentException("source name must not be blank");
        }
        if (!"object".equalsIgnoreCase(kind)) {
            throw new IllegalArgumentException("source '" + name + "' kind must be object");
        }
        if (provider == null) {
            throw new IllegalArgumentException("source '" + name + "' provider must not be blank");
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
