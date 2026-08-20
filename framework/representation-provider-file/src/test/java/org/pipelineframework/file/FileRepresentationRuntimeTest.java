package org.pipelineframework.file;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.ObjectSourceOperation;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectTargetRegistry;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.connector.objectingest.FilesystemObjectConnector;
import org.pipelineframework.connector.objectingest.FilesystemObjectSourceProvider;
import org.pipelineframework.connector.objectingest.FilesystemObjectTargetProvider;
import org.pipelineframework.repository.PayloadReference;

class FileRepresentationRuntimeTest {
    @Test
    void materializesNamedInputsForTypedServiceAndCleansSharedWorkspace() throws Exception {
        PayloadReference invoice = reference("incoming/invoice.pdf", 7);
        PayloadReference catalogue = reference("settings/config.yaml", 9);
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<Path> workspace = new AtomicReference<>();
        TestTarget target = new TestTarget(new AtomicReference<>());
        FileRepresentationRuntime runtime = new FileRepresentationRuntime(
            (reference, maxBytes) -> {
                calls.incrementAndGet();
                byte[] bytes = reference.equals(invoice) ? "invoice".getBytes() : "catalogue".getBytes();
                return CompletableFuture.completedFuture(new MaterializedPayload(
                    reference, bytes, reference.contentType(), reference.codec(), "checksum"));
            }, bindings(target), new PipelineYamlConfig(
                "example", "LOCAL", "COMPUTE", List.of(), Map.of(), Map.of(), Map.of(),
                List.of(), null, null, Map.of()), new ObjectTargetRegistry(List.of(target)));

        String result = runtime.withMaterialized(
            Map.of("invoice", invoice, "catalogue", catalogue), 32,
            paths -> {
                workspace.set(paths.get("invoice").getParent().getParent().getParent());
                try {
                    return Uni.createFrom().item(
                        Files.readString(paths.get("invoice")) + ":" + Files.readString(paths.get("catalogue")));
                } catch (java.io.IOException e) {
                    return Uni.createFrom().failure(e);
                }
            }).await().indefinitely();

        assertEquals("invoice:catalogue", result);
        assertEquals(2, calls.get());
        assertFalse(Files.exists(workspace.get()));
    }

    @Test
    void enforcesOneByteBudgetAcrossAllNamedInputs() {
        PayloadReference invoice = reference("invoice.pdf", 6);
        PayloadReference catalogue = reference("config.yaml", 6);
        TestTarget target = new TestTarget(new AtomicReference<>());
        FileRepresentationRuntime runtime = new FileRepresentationRuntime(
            (reference, maxBytes) -> CompletableFuture.completedFuture(new MaterializedPayload(
                reference, "123456".getBytes(), reference.contentType(), reference.codec(), "checksum")),
            bindings(target), new PipelineYamlConfig(
                "example", "LOCAL", "COMPUTE", List.of(), Map.of(), Map.of(), Map.of(),
                List.of(), null, null, Map.of()), new ObjectTargetRegistry(List.of(target)));

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> runtime.withMaterialized(
            Map.of("invoice", invoice, "catalogue", catalogue), 10,
            paths -> Uni.createFrom().item("unused")).await().indefinitely());

        assertTrue(failure.getMessage().contains("exceed maxBytes"));
    }

    @Test
    void materializesTrailingEmptyInputAfterBudgetIsConsumed() {
        PayloadReference invoice = reference("invoice.pdf", 3);
        PayloadReference emptyAttachment = reference("empty.txt", 0);
        AtomicReference<Long> emptyBudget = new AtomicReference<>();
        TestTarget target = new TestTarget(new AtomicReference<>());
        FileRepresentationRuntime runtime = new FileRepresentationRuntime(
            (reference, maxBytes) -> {
                byte[] bytes = reference.equals(invoice) ? "123".getBytes() : new byte[0];
                if (reference.equals(emptyAttachment)) {
                    emptyBudget.set(maxBytes);
                }
                return CompletableFuture.completedFuture(new MaterializedPayload(
                    reference, bytes, reference.contentType(), reference.codec(), "checksum"));
            }, bindings(target), new PipelineYamlConfig(
                "example", "LOCAL", "COMPUTE", List.of(), Map.of(), Map.of(), Map.of(),
                List.of(), null, null, Map.of()), new ObjectTargetRegistry(List.of(target)));
        Map<String, PayloadReference> inputs = new LinkedHashMap<>();
        inputs.put("invoice", invoice);
        inputs.put("attachment", emptyAttachment);

        String result = runtime.withMaterialized(inputs, 3,
            paths -> Uni.createFrom().item("materialized")).await().indefinitely();

        assertEquals("materialized", result);
        assertEquals(1L, emptyBudget.get());
    }

    @Test
    void cleansSharedWorkspaceWhenTypedServiceThrowsSynchronously() {
        PayloadReference invoice = reference("invoice.pdf", 7);
        AtomicReference<Path> workspace = new AtomicReference<>();
        TestTarget target = new TestTarget(new AtomicReference<>());
        FileRepresentationRuntime runtime = new FileRepresentationRuntime(
            (reference, maxBytes) -> CompletableFuture.completedFuture(new MaterializedPayload(
                reference, "invoice".getBytes(), reference.contentType(), reference.codec(), "checksum")),
            bindings(target), new PipelineYamlConfig(
                "example", "LOCAL", "COMPUTE", List.of(), Map.of(), Map.of(), Map.of(),
                List.of(), null, null, Map.of()), new ObjectTargetRegistry(List.of(target)));

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> runtime.withMaterialized(
            Map.of("invoice", invoice), 32,
            paths -> {
                workspace.set(paths.get("invoice").getParent().getParent().getParent());
                throw new IllegalStateException("service failed");
            }).await().indefinitely());

        assertEquals("service failed", failure.getMessage());
        assertFalse(Files.exists(workspace.get()));
    }

    @Test
    void composesTheExistingFilesystemConnectorThroughPortableReferences() throws Exception {
        Path inputRoot = Files.createTempDirectory("file-representation-input-");
        Path outputRoot = Files.createTempDirectory("file-representation-output-");
        try {
            Files.writeString(inputRoot.resolve("document.txt"), "portable");
            PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
                "documents", "object", "filesystem", Optional.of("documents"), Map.of("root", inputRoot.toString()),
                null, null, null, PipelineObjectPayloadConfig.reference());
            FilesystemObjectSourceProvider sourceProvider = new FilesystemObjectSourceProvider();
            PayloadReference raw = sourceProvider.list(source, 10).getFirst().contentRef();
            FilesystemObjectConnector connector = new FilesystemObjectConnector();
            ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
                List.of(new ConnectorBindingDefinition(
                    ConnectorBindingName.of("documents"), connector.id(), 1,
                    new ConnectorConfigurationDocument(Map.of()))),
                List.of(connector));
            bindings.start(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
            PayloadReference owned = bindings.ownPayloadReference(
                ConnectorBindingName.of("documents"), sourceProvider.id(), sourceProvider.majorVersion(), raw);
            PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
                "rendered", "object", "filesystem", Optional.of("documents"),
                Map.of("root", outputRoot.toString()), PipelineObjectNamingConfig.defaults(),
                PipelineObjectPublishPayloadConfig.defaults(), PipelineObjectPublishGroupingConfig.defaults());
            PipelineYamlConfig config = new PipelineYamlConfig(
                "example", "LOCAL", "COMPUTE", List.of(), Map.of(), Map.of(), Map.of("rendered", target),
                List.of(), null, null, Map.of());
            FileRepresentationRuntime runtime = new FileRepresentationRuntime(
                bindings::materialize, bindings, config,
                new ObjectTargetRegistry(List.of(new FilesystemObjectTargetProvider())));

            PayloadReference published = runtime.oneToOne(
                owned, 1024, "rendered", 1024, Optional.empty(), input -> {
                    try {
                        Path output = input.getParent().getParent().resolve("output/rendered.txt");
                        return Uni.createFrom().item(Files.writeString(output, Files.readString(input).toUpperCase()));
                    } catch (java.io.IOException e) {
                        return Uni.createFrom().failure(e);
                    }
                }).await().indefinitely();

            MaterializedPayload materialized = bindings.materialize(published, 1024).toCompletableFuture().join();
            assertArrayEquals("PORTABLE".getBytes(java.nio.charset.StandardCharsets.UTF_8), materialized.bytes());
        } finally {
            deleteTree(inputRoot);
            deleteTree(outputRoot);
        }
    }

    @Test
    void stagesInvokesPublishesAndCleansWorkspace() throws Exception {
        byte[] inputBytes = "input".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        PayloadReference input = reference("incoming/document.txt", inputBytes.length);
        AtomicReference<ObjectWriteRequest> written = new AtomicReference<>();
        AtomicReference<Path> workspace = new AtomicReference<>();
        TestTarget targetProvider = new TestTarget(written);
        ConnectorBindingRegistry bindings = bindings(targetProvider);
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "rendered", "object", "filesystem", Optional.of("documents"), Map.of(),
            PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(),
            PipelineObjectPublishGroupingConfig.defaults());
        PipelineYamlConfig config = new PipelineYamlConfig(
            "example", "LOCAL", "COMPUTE", List.of(), Map.of(), Map.of(), Map.of("rendered", target),
            List.of(), null, null, Map.of());
        FileRepresentationRuntime runtime = new FileRepresentationRuntime(
            (reference, maxBytes) -> CompletableFuture.completedFuture(new MaterializedPayload(
                reference, inputBytes, "text/plain", "raw", "input-checksum")),
            bindings,
            config,
            new ObjectTargetRegistry(List.of(targetProvider)));

        PayloadReference output = runtime.oneToOne(input, 1024, "rendered", 1024, Optional.empty(), path -> {
            workspace.set(path.getParent().getParent());
            try {
                Path rendered = Files.writeString(workspace.get().resolve("output/result.txt"), "output");
                return Uni.createFrom().item(rendered);
            } catch (java.io.IOException e) {
                return Uni.createFrom().failure(e);
            }
        }).await().indefinitely();

        assertFalse(Files.exists(workspace.get()));
        assertTrue(output.connectorOrigin().isPresent());
        assertTrue(output.connectorOrigin().orElseThrow().bindingName().equals(ConnectorBindingName.of("documents")));
        assertTrue("result.txt".equals(written.get().objectKey()));
        assertArrayEquals("output".getBytes(java.nio.charset.StandardCharsets.UTF_8), written.get().bytes());
    }

    private static ConnectorBindingRegistry bindings(TestTarget target) {
        ConnectorProvider<Void> provider = new ConnectorProvider<>() {
            private final ObjectSourceOperation source = new ObjectSourceOperation() {
                @Override
                public String id() {
                    return target.id();
                }

                @Override
                public CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
                    return CompletableFuture.failedFuture(new UnsupportedOperationException("not used"));
                }
            };

            @Override
            public ConnectorProviderId id() {
                return ConnectorProviderId.of("test.filesystem");
            }

            @Override
            public ConnectorProviderVersion version() {
                return new ConnectorProviderVersion(1, 0);
            }

            @Override
            public Collection<? extends ConnectorOperation> operations() {
                return List.of(source, target);
            }
        };
        return ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("documents"), provider.id(), 1, new ConnectorConfigurationDocument(Map.of()))),
            List.of(provider));
    }

    private static PayloadReference reference(String key, long size) {
        return new PayloadReference(
            "filesystem", "root", key, "text/plain", "raw", "checksum", size, "v1", Map.of(), Optional.empty());
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static final class TestTarget implements ObjectTargetProvider {
        private final AtomicReference<ObjectWriteRequest> written;

        private TestTarget(AtomicReference<ObjectWriteRequest> written) {
            this.written = written;
        }

        @Override
        public String providerName() {
            return "filesystem";
        }

        @Override
        public CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
            written.set(request);
            return CompletableFuture.completedFuture(new ObjectWriteResult(
                reference(request.objectKey(), request.bytes().length), request.bytes().length,
                request.checksum(), Instant.EPOCH));
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("write is overridden"));
        }
    }
}
