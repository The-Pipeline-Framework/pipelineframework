package org.pipelineframework.connector.objectingest;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.ObjectSourceOperation;
import org.pipelineframework.connector.ObjectTargetOperation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import software.amazon.awssdk.services.s3.S3Client;

class ObjectConnectorPackagingTest {
    @Test
    void eachBackendPackagesDistinctSourceAndTargetSemanticsUnderOneProviderIdentity() {
        assertOperations(new FilesystemObjectConnector(), "filesystem.objects", "filesystem");
        assertOperations(new StdioObjectConnector(new StandardStreams(
            new java.io.ByteArrayInputStream(new byte[0]), new java.io.ByteArrayOutputStream())), "stdio.objects", "stdio");
        S3ObjectConnector s3 = new S3ObjectConnector();
        try {
            assertOperations(s3, "s3.objects", "s3");
        } finally {
            s3.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        }
    }

    @Test
    void s3ConnectorStopCompletesAfterOwnedSourceExecutorTerminates() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            executor.submit(() -> {
                running.countDown();
                release.await();
                return null;
            });
            assertTrue(running.await(5, TimeUnit.SECONDS));
            S3Client client = mock(S3Client.class);
            S3ObjectConnector connector = new S3ObjectConnector(
                new S3ObjectSourceProvider(client, executor, true),
                new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024));

            var stopped = connector.stop(ConnectorRuntimeContext.empty()).toCompletableFuture();
            assertFalse(stopped.isDone());
            release.countDown();
            stopped.get(5, TimeUnit.SECONDS);
            assertTrue(executor.isTerminated());
        } finally {
            release.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void assertOperations(
        org.pipelineframework.connector.ConnectorProvider<?> provider,
        String providerId,
        String operationId
    ) {
        assertEquals(providerId, provider.id().value());
        assertEquals(2, provider.operations().size());
        assertEquals(Set.of(operationId), provider.operations().stream().map(ConnectorOperation::id).collect(Collectors.toSet()));
        ConnectorOperation source = assertInstanceOf(ObjectSourceOperation.class, provider.operations().stream()
            .filter(ObjectSourceOperation.class::isInstance).findFirst().orElseThrow());
        ConnectorOperation target = assertInstanceOf(ObjectTargetOperation.class, provider.operations().stream()
            .filter(ObjectTargetOperation.class::isInstance).findFirst().orElseThrow());
        assertNotSame(source, target);
    }
}
