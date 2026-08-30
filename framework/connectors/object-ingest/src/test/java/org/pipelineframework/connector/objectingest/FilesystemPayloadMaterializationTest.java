package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.ObjectSourceOperation;
import org.pipelineframework.connector.PayloadMaterializer;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;
import org.pipelineframework.repository.PayloadReference;

class FilesystemPayloadMaterializationTest {
    @TempDir
    Path tempDir;

    @Test
    void bindingOwnedFilesystemReferenceFeedsNeutralConsumerWithoutMapperGlue() throws Exception {
        byte[] expected = "portable payload".getBytes(StandardCharsets.UTF_8);
        Files.write(tempDir.resolve("document.txt"), expected);
        ConnectorBindingName bindingName = ConnectorBindingName.of("local-documents");
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                bindingName, ConnectorProviderId.of("filesystem.objects"), 1, ConnectorConfigurationDocument.empty())),
            List.of(new FilesystemObjectConnector()));
        bindings.activate(bindingName, ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        try {
            ObjectSourceProvider sourceOperation = (ObjectSourceProvider) bindings.requireOperation(
                bindingName, "filesystem", ConnectorOperationKind.OBJECT_SOURCE, 1);
            ObjectSourceItem item = sourceOperation.list(source(), 1).getFirst();
            PayloadReference reference = item.contentRef().withConnectorOrigin(
                bindings.objectSourceOrigin(bindingName, "filesystem", 1));
            NeutralConsumer consumer = new NeutralConsumer(bindings::materialize);

            MaterializedPayload payload = consumer.consume(reference, 1024);

            assertArrayEquals(expected, payload.bytes());
            assertEquals("raw", payload.codec());
            assertEquals(reference.checksum(), payload.checksum());

            Files.writeString(tempDir.resolve("document.txt"), "changed");
            CompletionException failure = assertThrows(
                CompletionException.class, () -> consumer.consume(reference, 1024));
            assertEquals("Filesystem payload checksum mismatch: document.txt", failure.getCause().getMessage());
        } finally {
            bindings.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        }
    }

    @Test
    void enforcesDeclaredMaximumBeforeOpeningFilesystemContent() throws Exception {
        Files.writeString(tempDir.resolve("large.txt"), "1234567890");
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();

        CompletionException failure = assertThrows(CompletionException.class, () ->
            provider.materialize(reference, 3).toCompletableFuture().join());

        assertEquals("Object exceeds configured maxBytes: large.txt", failure.getCause().getMessage());
    }

    @Test
    void rejectsModifiedFilesystemLocatorProvenance() throws Exception {
        byte[] expected = "portable payload".getBytes(StandardCharsets.UTF_8);
        Files.write(tempDir.resolve("document.txt"), expected);
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();

        Files.write(tempDir.resolve("other.txt"), expected);
        PayloadReference modifiedKey = copyWithLocator(reference, reference.container(), "other.txt");

        Path otherRoot = Files.createDirectory(tempDir.resolve("other-root"));
        Files.write(otherRoot.resolve("document.txt"), expected);
        PayloadReference modifiedContainer = copyWithLocator(reference, otherRoot.toRealPath().toString(), reference.key());

        CompletionException keyFailure = assertThrows(CompletionException.class, () ->
            provider.materialize(modifiedKey, 1024).toCompletableFuture().join());
        CompletionException containerFailure = assertThrows(CompletionException.class, () ->
            provider.materialize(modifiedContainer, 1024).toCompletableFuture().join());

        assertEquals(
            "Filesystem payload locator provenance mismatch: other.txt", keyFailure.getCause().getMessage());
        assertEquals(
            "Filesystem payload locator provenance mismatch: document.txt", containerFailure.getCause().getMessage());
    }

    @Test
    void rejectsInRootSymlinkThatEscapesCanonicalSourceRoot() throws Exception {
        byte[] expected = "portable payload".getBytes(StandardCharsets.UTF_8);
        Path sourceFile = tempDir.resolve("document.txt");
        Files.write(sourceFile, expected);
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();
        Path outside = Files.createTempFile("tpf-outside-", ".txt");
        try {
            Files.write(outside, expected);
            Files.delete(sourceFile);
            Files.createSymbolicLink(sourceFile, outside);

            CompletionException failure = assertThrows(CompletionException.class, () ->
                provider.materialize(reference, 1024).toCompletableFuture().join());

            assertEquals(
                "Filesystem object path escapes canonical root: document.txt", failure.getCause().getMessage());
        } finally {
            Files.deleteIfExists(outside);
        }
    }

    @Test
    void materializesOnProviderManagedExecutor() throws Exception {
        byte[] expected = "worker payload".getBytes(StandardCharsets.UTF_8);
        Files.write(tempDir.resolve("document.txt"), expected);
        AtomicReference<Runnable> scheduled = new AtomicReference<>();
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider(scheduled::set);
        PayloadReference reference = provider.list(source(), 1).getFirst().contentRef();

        CompletionStage<MaterializedPayload> pending = provider.materialize(reference, 1024);

        assertFalse(pending.toCompletableFuture().isDone());
        assertNotNull(scheduled.get());
        scheduled.get().run();
        assertArrayEquals(expected, pending.toCompletableFuture().join().bytes());

    }

    private PipelineObjectSourceConfig source() {
        return new PipelineObjectSourceConfig(
            "documents", "object", "filesystem", Map.of("root", tempDir.toString()), null, null, null, null);
    }

    private PayloadReference copyWithLocator(PayloadReference reference, String container, String key) {
        return new PayloadReference(
            reference.provider(),
            container,
            key,
            reference.contentType(),
            reference.codec(),
            reference.checksum(),
            reference.sizeBytes(),
            reference.version(),
            reference.metadata(),
            reference.connectorOrigin());
    }

    private record NeutralConsumer(PayloadMaterializer materializer) {
        private MaterializedPayload consume(PayloadReference reference, long maxBytes) {
            return materializer.materialize(reference, maxBytes).toCompletableFuture().join();
        }
    }
}
