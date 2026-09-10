package org.pipelineframework.connector.openapi.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OpenApiAcquisitionTest {
    private static final URI ROOT = URI.create("https://api.example.test/openapi.yaml");
    private static final URI TYPES = URI.create("https://api.example.test/contracts/types.yaml");

    @Test
    void vendorsAndRewritesAnAllowedHttpsReferenceClosure(@TempDir Path directory) throws Exception {
        Path snapshot = directory.resolve("contract/openapi.yaml");
        Map<URI, byte[]> documents = Map.of(
            ROOT, bytes("""
                openapi: 3.1.0
                info: {title: Evidence, version: '1'}
                paths: {}
                components:
                  schemas:
                    Evidence:
                      $ref: ./contracts/types.yaml#/Evidence
                """),
            TYPES, bytes("""
                Evidence:
                  type: object
                  properties:
                    value: {type: string}
                """));

        OpenApiAcquisition.Acquisition acquired = OpenApiAcquisition.acquire(
            ROOT, snapshot, Set.of(), uri -> documents.get(uri));
        OpenApiContractClosure.Resolved closure = OpenApiContractClosure.load(snapshot);

        assertEquals(2, acquired.documents().size());
        assertEquals(2, closure.documents().size());
        assertEquals(acquired.rootSha256(), closure.acquiredRootDigest());
        assertEquals(acquired.originalClosureSha256(), closure.acquiredClosureDigest());
        assertTrue(Files.readString(snapshot).contains("openapi.yaml.refs/"));
        assertTrue(Files.isRegularFile(OpenApiAcquisition.manifest(snapshot)));
        assertFalse(closure.digest().isBlank());
    }

    @Test
    void rejectsAReferenceOriginThatWasNotExplicitlyAllowed(@TempDir Path directory) {
        byte[] root = bytes("""
            openapi: 3.1.0
            info: {title: Evidence, version: '1'}
            paths: {}
            components:
              schemas:
                Evidence:
                  $ref: https://other.example.test/types.yaml#/Evidence
            """);

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> OpenApiAcquisition.acquire(ROOT, directory.resolve("openapi.yaml"), Set.of(), uri -> root));

        assertTrue(failure.getMessage().contains("not explicitly allowed"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
