package org.pipelineframework.invocation;

enum TransportBoundaryFailureCategory {
    NONE("none"),
    TIMEOUT("timeout"),
    AUTH("auth"),
    UNAVAILABLE("unavailable"),
    MALFORMED("malformed"),
    PROTOCOL("protocol"),
    CANCELLED("cancelled"),
    UNEXPECTED("unexpected");

    private final String metricValue;

    TransportBoundaryFailureCategory(String metricValue) {
        this.metricValue = metricValue;
    }

    String metricValue() {
        return metricValue;
    }
}
