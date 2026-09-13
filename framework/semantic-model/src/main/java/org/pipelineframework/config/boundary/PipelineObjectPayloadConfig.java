package org.pipelineframework.config.boundary;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Object payload admission settings.
 *
 * @param mode payload mode: metadata, reference, or text
 * @param refField optional emitted domain field that should receive a payload reference
 * @param maxBytes maximum bytes loaded for text payload mode
 * @param charset text payload charset
 */
public record PipelineObjectPayloadConfig(
    String mode,
    String refField,
    long maxBytes,
    Charset charset
) {
    public PipelineObjectPayloadConfig {
        mode = normalize(mode);
        mode = mode == null ? "reference" : mode;
        refField = normalize(refField);
        charset = charset == null ? StandardCharsets.UTF_8 : charset;
        if (!"metadata".equalsIgnoreCase(mode)
            && !"reference".equalsIgnoreCase(mode)
            && !"text".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("object source payload.mode must be metadata, reference, or text");
        }
        if (maxBytes < 0) {
            throw new IllegalArgumentException("object source payload.maxBytes must be >= 0");
        }
    }

    public static PipelineObjectPayloadConfig reference() {
        return new PipelineObjectPayloadConfig("reference", null, 0L, StandardCharsets.UTF_8);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
