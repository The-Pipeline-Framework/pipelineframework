package org.pipelineframework.connector.http;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** One deterministic wire-input path to HTTP parameter projection. */
public record HttpParameterPin(
    String name,
    HttpParameterLocation location,
    String sourcePath,
    HttpParameterStyle style,
    boolean explode,
    boolean required,
    boolean allowReserved,
    HttpWireSchema schema
) {
    public HttpParameterPin {
        name = requireText(name, "HTTP parameter name");
        location = Objects.requireNonNull(location, "HTTP parameter location must not be null");
        sourcePath = recordPath(sourcePath);
        style = Objects.requireNonNull(style, "HTTP parameter style must not be null");
        schema = Objects.requireNonNull(schema, "HTTP parameter schema must not be null");
        if (location == HttpParameterLocation.PATH && !required) {
            throw new IllegalArgumentException("HTTP path parameters must be required");
        }
        if (allowReserved && location != HttpParameterLocation.QUERY) {
            throw new IllegalArgumentException("allowReserved is supported only for query parameters");
        }
        if (!compatible(location, style)) {
            throw new IllegalArgumentException("HTTP parameter style " + style + " is invalid for " + location);
        }
        validateShape(location, style, explode, schema);
        if (location == HttpParameterLocation.HEADER && reservedHeader(name)) {
            throw new IllegalArgumentException("HTTP operation pins cannot declare reserved header '" + name + "'");
        }
    }

    public static boolean reservedHeader(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "authorization", "cookie", "host", "content-length", "content-type", "connection",
                "expect", "upgrade", "proxy-authorization", "transfer-encoding" -> true;
            default -> false;
        };
    }

    private static boolean compatible(HttpParameterLocation location, HttpParameterStyle style) {
        return switch (location) {
            case PATH -> style == HttpParameterStyle.SIMPLE || style == HttpParameterStyle.LABEL
                || style == HttpParameterStyle.MATRIX;
            case QUERY -> style == HttpParameterStyle.FORM || style == HttpParameterStyle.SPACE_DELIMITED
                || style == HttpParameterStyle.PIPE_DELIMITED || style == HttpParameterStyle.DEEP_OBJECT;
            case HEADER -> style == HttpParameterStyle.SIMPLE;
            case COOKIE -> style == HttpParameterStyle.FORM;
        };
    }

    private static void validateShape(
        HttpParameterLocation location,
        HttpParameterStyle style,
        boolean explode,
        HttpWireSchema schema
    ) {
        String type = schema.node().path("type").asText();
        if (!Set.of("string", "integer", "number", "boolean", "array", "object").contains(type)) {
            throw new IllegalArgumentException("HTTP parameter schema requires one concrete serializable type");
        }
        if ((style == HttpParameterStyle.SPACE_DELIMITED || style == HttpParameterStyle.PIPE_DELIMITED)
            && (!"array".equals(type) || explode)) {
            throw new IllegalArgumentException(style + " query parameters require an array with explode=false");
        }
        if (style == HttpParameterStyle.DEEP_OBJECT && (!"object".equals(type) || !explode)) {
            throw new IllegalArgumentException("DEEP_OBJECT query parameters require an object with explode=true");
        }
    }

    static String recordPath(String value) {
        String result = requireText(value, "wire source path");
        if (!result.matches("[A-Za-z][A-Za-z0-9_]*(?:\\.[A-Za-z][A-Za-z0-9_]*)*")) {
            throw new IllegalArgumentException("wire source path must traverse record fields only: " + result);
        }
        return result;
    }

    static String requireText(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
