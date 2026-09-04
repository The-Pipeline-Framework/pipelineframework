package org.pipelineframework.processor.renderer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.squareup.javapoet.CodeBlock;

/** Emits deterministic Java literals for compiler-validated YAML configuration values. */
final class JavaPoetLiteral {
    private JavaPoetLiteral() {
    }

    static CodeBlock value(Object value) {
        if (value instanceof String string) {
            return CodeBlock.of("$S", string);
        }
        if (value instanceof Boolean bool) {
            return CodeBlock.of("$L", bool);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return CodeBlock.of("$L", value);
        }
        if (value instanceof Long number) {
            return CodeBlock.of("$LL", number);
        }
        if (value instanceof Float number) {
            if (!Float.isFinite(number)) {
                return nonFinite(number, Float.class);
            }
            return CodeBlock.of("$Lf", number);
        }
        if (value instanceof Double number) {
            if (!Double.isFinite(number)) {
                return nonFinite(number, Double.class);
            }
            return CodeBlock.of("$Ld", number);
        }
        if (value instanceof BigInteger number) {
            return CodeBlock.of("new $T($S)", BigInteger.class, number.toString());
        }
        if (value instanceof BigDecimal number) {
            return CodeBlock.of("new $T($S)", BigDecimal.class, number.toPlainString());
        }
        if (value instanceof Map<?, ?> map) {
            return map(map);
        }
        if (value instanceof List<?> list) {
            CodeBlock.Builder values = CodeBlock.builder();
            for (int index = 0; index < list.size(); index++) {
                if (index > 0) {
                    values.add(", ");
                }
                values.add("$L", value(list.get(index)));
            }
            return CodeBlock.of("$T.of($L)", List.class, values.build());
        }
        throw new IllegalArgumentException("Unsupported generated configuration value: "
            + value.getClass().getName());
    }

    private static CodeBlock nonFinite(Number value, Class<?> type) {
        String constant = Double.isNaN(value.doubleValue())
            ? "NaN"
            : value.doubleValue() > 0 ? "POSITIVE_INFINITY" : "NEGATIVE_INFINITY";
        return CodeBlock.of("$T.$L", type, constant);
    }

    private static CodeBlock map(Map<?, ?> map) {
        if (map.isEmpty()) {
            return CodeBlock.of("$T.of()", Map.class);
        }
        CodeBlock.Builder entries = CodeBlock.builder();
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet().stream()
            .sorted(java.util.Comparator.comparing(candidate -> String.valueOf(candidate.getKey())))
            .toList()) {
            if (!first) {
                entries.add(", ");
            }
            entries.add("$T.entry($S, $L)", Map.class, String.valueOf(entry.getKey()), value(entry.getValue()));
            first = false;
        }
        return CodeBlock.of("$T.ofEntries($L)", Map.class, entries.build());
    }
}
