package org.pipelineframework.representation.spi;

import java.util.Objects;

/** Source-neutral compiler request describing one selected Connector operation boundary. */
public record OperationBoundaryRequest(
    String boundaryIdentity,
    String connectorProviderId,
    int connectorProviderMajorVersion,
    String operationId,
    String operationKind,
    int operationMajorVersion,
    CanonicalType inputType,
    CanonicalType outputType
) {
    public OperationBoundaryRequest {
        boundaryIdentity = text(boundaryIdentity, "operation boundary identity");
        connectorProviderId = text(connectorProviderId, "Connector provider ID");
        if (connectorProviderMajorVersion < 1) {
            throw new IllegalArgumentException("Connector provider major version must be positive");
        }
        operationId = text(operationId, "Connector operation ID");
        operationKind = text(operationKind, "Connector operation kind");
        if (operationMajorVersion < 1) {
            throw new IllegalArgumentException("Connector operation major version must be positive");
        }
        inputType = Objects.requireNonNull(inputType, "operation canonical input type must not be null");
        outputType = Objects.requireNonNull(outputType, "operation canonical output type must not be null");
    }

    private static String text(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
