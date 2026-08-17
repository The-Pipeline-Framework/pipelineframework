/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.plugin.repository;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.repository.RepositoryChecksums;
import org.pipelineframework.repository.RepositoryReadResult;
import org.pipelineframework.repository.RepositoryWriteRequest;

class MaterializationServiceTest {

    @Test
    void adaptsRepositoryReadResultToNeutralMaterializedPayload() {
        TestRepositoryManager repositoryManager = new TestRepositoryManager();
        byte[] expected = "portable repository payload".getBytes(StandardCharsets.UTF_8);
        String checksum = RepositoryChecksums.sha256Hex(expected);
        PayloadReference reference = repositoryManager.store(new RepositoryWriteRequest(
            "payloads", "document", expected, "text/plain", "raw", checksum, null, Map.of()))
            .await().indefinitely();

        var materialized = repositoryManager.materialize(reference, 1024).toCompletableFuture().join();

        assertArrayEquals(expected, materialized.bytes());
        assertEquals(reference, materialized.reference());
        assertEquals(checksum, materialized.checksum());
    }

    @Test
    void rejectsRepositoryBytesThatDoNotMatchReferenceChecksum() {
        TestRepositoryManager repositoryManager = new TestRepositoryManager();
        byte[] expected = "expected".getBytes(StandardCharsets.UTF_8);
        PayloadReference reference = repositoryManager.store(new RepositoryWriteRequest(
            "payloads", "document", expected, "text/plain", "raw", RepositoryChecksums.sha256Hex(expected), null,
            Map.of())).await().indefinitely();
        repositoryManager.payloads.put(reference, "changed".getBytes(StandardCharsets.UTF_8));

        CompletionException failure = assertThrows(CompletionException.class, () ->
            repositoryManager.materialize(reference, 1024).toCompletableFuture().join());

        assertEquals(
            "materialized repository payload checksum mismatch for document", failure.getCause().getMessage());
    }

    @Test
    void rejectsRepositoryResultForDifferentReference() {
        TestRepositoryManager repositoryManager = new TestRepositoryManager();
        byte[] expected = "expected".getBytes(StandardCharsets.UTF_8);
        PayloadReference reference = repositoryManager.store(new RepositoryWriteRequest(
            "payloads", "document", expected, "text/plain", "raw", RepositoryChecksums.sha256Hex(expected), null,
            Map.of())).await().indefinitely();
        repositoryManager.loadedReference = Optional.of(new PayloadReference(
            reference.provider(),
            reference.container(),
            "different-document",
            reference.contentType(),
            reference.codec(),
            reference.checksum(),
            reference.sizeBytes(),
            reference.version(),
            reference.metadata(),
            reference.connectorOrigin()));

        CompletionException failure = assertThrows(CompletionException.class, () ->
            repositoryManager.materialize(reference, 1024).toCompletableFuture().join());

        assertEquals("repository materialized a different payload reference", failure.getCause().getMessage());
    }

    @Test
    void referencesAndDereferencesConfiguredFields() {
        TestRepositoryManager repositoryManager = new TestRepositoryManager();
        MaterializationService service = new MaterializationService();
        service.repositoryManager = repositoryManager;
        ParsedDocument document = new ParsedDocument("doc-1", "text that should be stored out of line", null);

        ParsedDocument referenced = service.referenceFields(
                document,
                ParsedDocument.class,
                Map.of("text", "textRef"),
                1,
                "documents",
                "v1")
            .await().indefinitely();

        assertNull(referenced.text());
        assertNotNull(referenced.textRef());
        assertEquals("documents", referenced.textRef().container());
        assertEquals("text", referenced.textRef().metadata().get("field"));

        ParsedDocument hydrated = service.dereferenceFields(
                referenced,
                ParsedDocument.class,
                Map.of("text", "textRef"))
            .await().indefinitely();

        assertEquals(document.text(), hydrated.text());
        assertEquals(referenced.textRef(), hydrated.textRef());
    }

    @Test
    void skipsReferenceWhenFieldIsBelowThreshold() {
        TestRepositoryManager repositoryManager = new TestRepositoryManager();
        MaterializationService service = new MaterializationService();
        service.repositoryManager = repositoryManager;
        ParsedDocument document = new ParsedDocument("doc-1", "small", null);

        ParsedDocument result = service.referenceFields(
                document,
                ParsedDocument.class,
                Map.of("text", "textRef"),
                100,
                "documents",
                "v1")
            .await().indefinitely();

        assertEquals("small", result.text());
        assertNull(result.textRef());
        assertEquals(0, repositoryManager.payloads.size());
    }

    @Test
    void referencesAndDereferencesByteFields() {
        TestRepositoryManager repositoryManager = new TestRepositoryManager();
        MaterializationService service = new MaterializationService();
        service.repositoryManager = repositoryManager;
        byte[] payload = "binary payload".getBytes(StandardCharsets.UTF_8);
        ParsedBinaryDocument document = new ParsedBinaryDocument("doc-1", payload, null);

        ParsedBinaryDocument referenced = service.referenceFields(
                document,
                ParsedBinaryDocument.class,
                Map.of("data", "dataRef"),
                1,
                "documents",
                "v1")
            .await().indefinitely();

        assertNull(referenced.data());
        assertNotNull(referenced.dataRef());
        assertEquals("documents", referenced.dataRef().container());
        assertEquals("data", referenced.dataRef().metadata().get("field"));

        ParsedBinaryDocument hydrated = service.dereferenceFields(
                referenced,
                ParsedBinaryDocument.class,
                Map.of("data", "dataRef"))
            .await().indefinitely();

        assertArrayEquals(payload, hydrated.data());
        assertEquals(referenced.dataRef(), hydrated.dataRef());
    }

    private record ParsedDocument(String docId, String text, PayloadReference textRef) {
    }

    private record ParsedBinaryDocument(String docId, byte[] data, PayloadReference dataRef) {
    }

    private static final class TestRepositoryManager extends RepositoryManager {
        private final Map<PayloadReference, byte[]> payloads = new HashMap<>();
        private Optional<PayloadReference> loadedReference = Optional.empty();

        @Override
        public Uni<PayloadReference> store(RepositoryWriteRequest request) {
            PayloadReference reference = new PayloadReference(
                "test",
                request.container(),
                request.key(),
                request.contentType(),
                request.codec(),
                request.checksum(),
                request.payload().length,
                request.version(),
                request.metadata(),
                Optional.empty());
            payloads.put(reference, request.payload());
            return Uni.createFrom().item(reference);
        }

        @Override
        public Uni<RepositoryReadResult> load(PayloadReference reference) {
            PayloadReference resultReference = loadedReference.orElse(reference);
            return Uni.createFrom().item(new RepositoryReadResult(
                resultReference,
                payloads.get(reference),
                resultReference.contentType(),
                resultReference.codec(),
                resultReference.checksum()));
        }
    }
}
