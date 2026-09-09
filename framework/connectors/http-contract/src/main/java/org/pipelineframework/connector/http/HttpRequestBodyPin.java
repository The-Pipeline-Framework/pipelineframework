package org.pipelineframework.connector.http;

import java.util.Objects;
import java.util.Optional;

/** Selected request representation; absent sourcePath means the complete normalized wire input. */
public record HttpRequestBodyPin(
    String mediaType,
    Optional<String> sourcePath,
    boolean required,
    HttpWireSchema schema
) {
    public HttpRequestBodyPin {
        mediaType = mediaType(mediaType);
        sourcePath = Objects.requireNonNull(sourcePath, "request body source path must not be null")
            .map(HttpParameterPin::recordPath);
        schema = Objects.requireNonNull(schema, "request body schema must not be null");
    }

    static String mediaType(String value) {
        String result = HttpParameterPin.requireText(value, "HTTP media type").toLowerCase(java.util.Locale.ROOT);
        if (!("application/json".equals(result) || result.startsWith("application/") && result.endsWith("+json"))) {
            throw new IllegalArgumentException("initial pinned HTTP support requires JSON or +json media types: " + value);
        }
        return result;
    }
}
