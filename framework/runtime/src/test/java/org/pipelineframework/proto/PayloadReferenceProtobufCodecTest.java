/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.pipelineframework.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorPayloadOrigin;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.ObjectSourceOperation;
import org.pipelineframework.repository.PayloadReference;

class PayloadReferenceProtobufCodecTest {
    private static final ConnectorBindingName BINDING = ConnectorBindingName.of("documents");
    private static final String V1_FIXTURE =
        "CgpmaWxlc3lzdGVtEgovZG9jdW1lbnRzGhRpbmNvbWluZy9pbnZvaWNlLnBkZiIPYXBwbGljYXRpb24vcGRm"
            + "KgNyYXcyDnNoYTI1NjpwYXlsb2FkOIABQg5vYmplY3QtdmVyc2lvbkoTCgRldGFnEgtvYmplY3QtZXRhZ0of"
            + "Cg1sb2NhdG9yRGlnZXN0Eg5sb2NhdG9yLWRpZ2VzdFKNAQoJZG9jdW1lbnRzEhJmaWxlc3lzdGVtLm9iamVj"
            + "dHMaCmZpbGVzeXN0ZW0iEXRwZjpvYmplY3Qtc291cmNlKAEwATgBQhlmaWxlc3lzdGVtLm9iamVjdHMuY29u"
            + "ZmlnSAFSFGNvbmZpZ3VyYXRpb24tZGlnZXN0WhRkb2N1bWVudHMtY29ubmVjdGlvbg==";

    @Test
    void roundTripsRepositoryOwnedReferenceWithoutConnectorOrigin() {
        PayloadReference reference = new PayloadReference(
            "repository", "invoices", "2026/invoice.pdf", "application/pdf", "raw", "sha256:abc", 42,
            "version-1", Map.of("source", "upload"), Optional.empty());

        assertEquals(reference, PayloadReferenceProtobufCodec.decode(PayloadReferenceProtobufCodec.encode(reference)));
    }

    @Test
    void roundTripsCompleteConnectorOwnedReferenceWithoutResolvedSecrets() {
        PayloadReference reference = connectorReference();

        byte[] encoded = PayloadReferenceProtobufCodec.encode(reference);
        PayloadReference decoded = PayloadReferenceProtobufCodec.decode(encoded);

        assertEquals(reference, decoded);
        String inspectable = new String(encoded, StandardCharsets.ISO_8859_1);
        assertTrue(inspectable.contains("documents-connection"));
        assertFalse(inspectable.contains("resolved-secret"));
    }

    @Test
    void roundTripsConnectorOwnedReferenceWithAbsentOptionalLocatorAndContentFields() {
        ConnectorPayloadOrigin origin = connectorReference().connectorOrigin().orElseThrow();
        PayloadReference reference = new PayloadReference(
            null, null, "invoice.pdf", null, null, null, 0, null, Map.of(), Optional.of(origin));

        assertEquals(reference, PayloadReferenceProtobufCodec.decode(PayloadReferenceProtobufCodec.encode(reference)));
    }

    @Test
    void equalReferencesHaveDeterministicStorageBytes() {
        Map<String, String> forward = new LinkedHashMap<>();
        forward.put("alpha", "one");
        forward.put("beta", "two");
        Map<String, String> reverse = new LinkedHashMap<>();
        reverse.put("beta", "two");
        reverse.put("alpha", "one");
        PayloadReference first = repositoryReference(forward);
        PayloadReference second = repositoryReference(reverse);

        assertEquals(first, second);
        assertArrayEquals(
            PayloadReferenceProtobufCodec.encode(first), PayloadReferenceProtobufCodec.encode(second));
    }

    @Test
    void retainsPublishedVersionOneFixture() {
        byte[] encoded = PayloadReferenceProtobufCodec.encode(connectorReference());

        assertEquals(V1_FIXTURE, Base64.getEncoder().encodeToString(encoded));
        assertEquals(connectorReference(), PayloadReferenceProtobufCodec.decode(Base64.getDecoder().decode(V1_FIXTURE)));
    }

    @Test
    void ignoresCompatibleUnknownProtobufFields() throws Exception {
        PayloadReference reference = connectorReference();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(PayloadReferenceProtobufCodec.encode(reference));
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.writeString(99, "future-compatible-field");
        output.flush();

        assertEquals(reference, PayloadReferenceProtobufCodec.decode(bytes.toByteArray()));
    }

    @Test
    void rejectsMalformedOrIncompleteProtobuf() {
        assertThrows(IllegalArgumentException.class, () ->
            PayloadReferenceProtobufCodec.decode(new byte[] {(byte) 0xff}));
        assertThrows(IllegalArgumentException.class, () ->
            PayloadReferenceProtobufCodec.decode(new byte[] {10, 0}));
    }

    @Test
    void decodedReferenceMaterializesThroughCompatibleActiveBinding() {
        ConnectorBindingRegistry registry = registry();
        registry.start(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        try {
            PayloadReference owned = registry.ownPayloadReference(
                BINDING,
                "read",
                1,
                new PayloadReference(
                    "test", "/documents", "invoice.pdf", "application/pdf", "raw", "sum", 3, "v1",
                    Map.of("locator", "stable"), Optional.empty()));
            PayloadReference decoded = PayloadReferenceProtobufCodec.decode(
                PayloadReferenceProtobufCodec.encode(owned));

            MaterializedPayload materialized = registry.materialize(decoded, 3).toCompletableFuture().join();

            assertEquals(owned, materialized.reference());
            assertArrayEquals(new byte[] {1, 2, 3}, materialized.bytes());
        } finally {
            registry.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        }
    }

    private static PayloadReference connectorReference() {
        ConnectorPayloadOrigin origin = new ConnectorPayloadOrigin(
            BINDING,
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("filesystem.objects"),
                "filesystem",
                ConnectorOperationKind.OBJECT_SOURCE,
                1),
            1,
            Optional.of(new ConnectorConfigurationSnapshot(
                "filesystem.objects.config",
                1,
                "configuration-digest",
                List.of(new ConnectionRef("documents-connection")))));
        return new PayloadReference(
            "filesystem",
            "/documents",
            "incoming/invoice.pdf",
            "application/pdf",
            "raw",
            "sha256:payload",
            128,
            "object-version",
            Map.of("locatorDigest", "locator-digest", "etag", "object-etag"),
            Optional.of(origin));
    }

    private static PayloadReference repositoryReference(Map<String, String> metadata) {
        return new PayloadReference(
            "repository", "invoices", "invoice.pdf", "application/pdf", "raw", "sha256:abc", 42,
            "version-1", metadata, Optional.empty());
    }

    private static ConnectorBindingRegistry registry() {
        return ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                BINDING,
                ConnectorProviderId.of("test.documents"),
                1,
                new ConnectorConfigurationDocument(Map.of("root", "/documents")))),
            List.of(new TestProvider()));
    }

    public record ProviderConfig(String root) {
    }

    public static final class TestProvider implements ConnectorProvider<ProviderConfig> {
        private final ObjectSourceOperation operation = new ObjectSourceOperation() {
            @Override
            public String id() {
                return "read";
            }

            @Override
            public CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
                return CompletableFuture.completedFuture(new MaterializedPayload(
                    reference, new byte[] {1, 2, 3}, reference.contentType(), reference.codec(), reference.checksum()));
            }
        };

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("test.documents");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Optional<ConnectorConfigSchema<ProviderConfig>> configurationSchema() {
            return Optional.of(ConnectorConfigSchema.record(ProviderConfig.class, "test.documents.config", 1));
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(operation);
        }
    }
}
