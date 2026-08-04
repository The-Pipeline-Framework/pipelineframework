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

package org.pipelineframework.plugin.repository.provider;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.repository.RepositoryChecksums;
import org.pipelineframework.repository.RepositoryWriteRequest;

class FilesystemRepositoryProviderTest {

    @TempDir
    Path tempDir;

    @Test
    void storesLoadsChecksAndDeletesPayloads() {
        FilesystemRepositoryProvider provider = new FilesystemRepositoryProvider();
        provider.root = tempDir.toString();
        provider.verifyChecksum = true;
        byte[] payload = "large parsed text".getBytes(StandardCharsets.UTF_8);
        String checksum = RepositoryChecksums.sha256Hex(payload);

        PayloadReference reference = provider.store(new RepositoryWriteRequest(
                "documents",
                "ab/" + checksum,
                payload,
                "text/plain; charset=utf-8",
                "string",
                checksum,
                "v1",
                Map.of("field", "text")))
            .await().indefinitely();

        assertEquals("filesystem", reference.provider());
        assertEquals("documents", reference.container());
        assertEquals(payload.length, reference.sizeBytes());
        assertTrue(provider.exists(reference).await().indefinitely());
        assertArrayEquals(payload, provider.load(reference).await().indefinitely().payload());
        assertTrue(provider.delete(reference).await().indefinitely());
        assertFalse(provider.exists(reference).await().indefinitely());
    }

    @Test
    void rejectsKeysThatEscapeTheRepositoryRoot() {
        FilesystemRepositoryProvider provider = new FilesystemRepositoryProvider();
        provider.root = tempDir.toString();

        RepositoryWriteRequest request = new RepositoryWriteRequest(
            null,
            "../escape",
            "x".getBytes(StandardCharsets.UTF_8),
            "text/plain",
            "string",
            null,
            null,
            Map.of());

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> provider.store(request).await().indefinitely());

        assertTrue(exception.getMessage().contains("escapes filesystem root"));
    }

    @Test
    void verifiesChecksumOnLoad() {
        FilesystemRepositoryProvider provider = new FilesystemRepositoryProvider();
        provider.root = tempDir.toString();
        provider.verifyChecksum = true;
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

        PayloadReference reference = provider.store(new RepositoryWriteRequest(
                null,
                "payload",
                payload,
                "text/plain",
                "string",
                "bad-checksum",
                null,
                Map.of()))
            .await().indefinitely();

        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> provider.load(reference).await().indefinitely());

        assertTrue(exception.getMessage().contains("checksum mismatch"));
    }
}
