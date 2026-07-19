package org.pipelineframework.invocation;

enum TransportBoundaryFailureCategory {
    NONE("none"),
    TIMEOUT("timeout"),
    AUTH("auth"),
    UNAVAILABLE("unavailable"),
    MALFORMED("malformed"),
    PROTOCOL("protocol"),
    CANCELLED("cancelled"),
    REMOTE_SERVER("remote_server"),
    CIRCUIT_OPEN("circuit_open"),
    UNEXPECTED("unexpected");

    private final String metricValue;

    TransportBoundaryFailureCategory(String metricValue) {
        this.metricValue = metricValue;
    }

    String metricValue() {
        return metricValue;
    }
}
