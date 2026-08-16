package org.pipelineframework.connector.objectingest;

import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ObjectSourceOperation;
import org.pipelineframework.connector.ObjectTargetOperation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ObjectConnectorPackagingTest {
    @Test
    void eachBackendPackagesDistinctSourceAndTargetSemanticsUnderOneProviderIdentity() {
        assertOperations(new FilesystemObjectConnector(), "filesystem.objects", "filesystem");
        assertOperations(new StdioObjectConnector(new StandardStreams(
            new java.io.ByteArrayInputStream(new byte[0]), new java.io.ByteArrayOutputStream())), "stdio.objects", "stdio");
        assertOperations(new S3ObjectConnector(), "s3.objects", "s3");
    }

    private static void assertOperations(
        org.pipelineframework.connector.ConnectorProvider<?> provider,
        String providerId,
        String operationId
    ) {
        assertEquals(providerId, provider.id().value());
        assertEquals(Set.of(operationId), provider.operations().stream().map(ConnectorOperation::id).collect(Collectors.toSet()));
        assertInstanceOf(ObjectSourceOperation.class, provider.operations().stream()
            .filter(ObjectSourceOperation.class::isInstance).findFirst().orElseThrow());
        assertInstanceOf(ObjectTargetOperation.class, provider.operations().stream()
            .filter(ObjectTargetOperation.class::isInstance).findFirst().orElseThrow());
    }
}
