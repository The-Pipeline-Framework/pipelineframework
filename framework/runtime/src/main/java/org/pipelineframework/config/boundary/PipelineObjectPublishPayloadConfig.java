package org.pipelineframework.config.boundary;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Object publish payload settings.
 *
 * @param contentType content type applied to written objects
 * @param charset text charset hint for payload renderers
 */
public record PipelineObjectPublishPayloadConfig(
    String contentType,
    Charset charset
) {
    public PipelineObjectPublishPayloadConfig {
        contentType = normalize(contentType);
        contentType = contentType == null ? "application/octet-stream" : contentType;
        charset = charset == null ? StandardCharsets.UTF_8 : charset;
    }

    public static PipelineObjectPublishPayloadConfig defaults() {
        return new PipelineObjectPublishPayloadConfig("application/octet-stream", StandardCharsets.UTF_8);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
