package org.pipelineframework.query;

final class QueryFailureCode {
    private QueryFailureCode() {
    }

    static String require(String value) {
        if (value == null || !value.matches("[a-z][a-z0-9-]{0,127}")) {
            throw new IllegalArgumentException("invalid query outcome code: " + value);
        }
        return value;
    }
}
