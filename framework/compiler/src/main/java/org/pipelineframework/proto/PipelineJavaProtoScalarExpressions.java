package org.pipelineframework.proto;

import java.util.Objects;
import java.util.function.UnaryOperator;

/** Shared Java source expressions for canonical scalar conversion at protobuf boundaries. */
public final class PipelineJavaProtoScalarExpressions {

    private PipelineJavaProtoScalarExpressions() {
    }

    public static String toProto(
        String scalar,
        String expression,
        UnaryOperator<String> payloadReferenceConverter
    ) {
        Objects.requireNonNull(payloadReferenceConverter, "payloadReferenceConverter must not be null");
        return switch (scalar) {
            case "decimal" -> expression + ".toPlainString()";
            case "uuid", "timestamp", "datetime", "date", "duration", "currency", "uri", "path" ->
                expression + ".toString()";
            case "payload_ref" -> payloadReferenceConverter.apply(expression);
            default -> expression;
        };
    }

    public static String fromProto(
        String scalar,
        String expression,
        UnaryOperator<String> payloadReferenceConverter
    ) {
        Objects.requireNonNull(payloadReferenceConverter, "payloadReferenceConverter must not be null");
        return switch (scalar) {
            case "decimal" -> "new java.math.BigDecimal(" + expression + ")";
            case "uuid" -> "java.util.UUID.fromString(" + expression + ")";
            case "timestamp" -> "java.time.Instant.parse(" + expression + ")";
            case "datetime" -> "java.time.LocalDateTime.parse(" + expression + ")";
            case "date" -> "java.time.LocalDate.parse(" + expression + ")";
            case "duration" -> "java.time.Duration.parse(" + expression + ")";
            case "currency" -> "java.util.Currency.getInstance(" + expression + ")";
            case "uri" -> "java.net.URI.create(" + expression + ")";
            case "path" -> "java.nio.file.Path.of(" + expression + ")";
            case "payload_ref" -> payloadReferenceConverter.apply(expression);
            default -> expression;
        };
    }
}
