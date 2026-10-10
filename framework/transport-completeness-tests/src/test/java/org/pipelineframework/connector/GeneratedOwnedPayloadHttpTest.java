package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Exercises compiler-shaped resources through Quarkus REST and a real filesystem binding. */
@QuarkusTest
@QuarkusTestResource(value = OwnedPayloadHttpTestResource.class, restrictToAnnotatedClass = true)
class GeneratedOwnedPayloadHttpTest {
    @TestHTTPResource("/")
    URI base;

    @Inject
    ObjectMapper mapper;

    @Test
    @TestSecurity(user = "alice")
    void uploadReturnsOwnedReferenceAndAuthorisedDownloadStreamsIt() throws Exception {
        byte[] payload = "invoice content".getBytes(StandardCharsets.UTF_8);
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> uploaded = client.send(uploadRequest(payload, "application/pdf"),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, uploaded.statusCode(), uploaded.body());
            var reference = mapper.readTree(uploaded.body()).get("payload_ref");
            assertEquals(payload.length, reference.get("sizeBytes").asLong());
            assertEquals("application/pdf", reference.get("contentType").asText());
            assertTrue(reference.hasNonNull("connectorOrigin"));

            HttpResponse<String> admitted = client.send(ownedRequest("/test/owned-payload/admit")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"payload_ref\":" + reference + "}"))
                .build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, admitted.statusCode(), admitted.body());
            var resultReference = mapper.readTree(admitted.body()).get("payload_ref");
            assertEquals(reference, resultReference);

            HttpResponse<byte[]> downloaded = client.send(ownedRequest("/tpf/payloads/receipt/download")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(resultReference.toString())).build(),
                HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, downloaded.statusCode());
            assertArrayEquals(payload, downloaded.body());
            assertEquals("application/pdf", downloaded.headers().firstValue("Content-Type").orElseThrow());
            assertEquals(Long.toString(payload.length),
                downloaded.headers().firstValue("Content-Length").orElseThrow());
            assertEquals("private, no-store", downloaded.headers().firstValue("Cache-Control").orElseThrow());

            HttpResponse<String> denied = client.send(HttpRequest.newBuilder(base.resolve("/tpf/payloads/receipt/download"))
                .header("X-Tenant-Id", "tenant-b")
                .header("X-Scope-Id", "invoice-1")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(reference.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(403, denied.statusCode());

            HttpResponse<String> missingOwner = client.send(HttpRequest.newBuilder(base.resolve("/tpf/payloads/receipt/download"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(reference.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(403, missingOwner.statusCode());

            ObjectNode forged = ((ObjectNode) reference.deepCopy());
            forged.put("key", reference.get("key").asText() + "-forged");
            HttpResponse<String> rejected = client.send(ownedRequest("/tpf/payloads/receipt/download")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(forged.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(403, rejected.statusCode());

            HttpResponse<String> range = client.send(ownedRequest("/tpf/payloads/receipt/download")
                .header("Content-Type", "application/json")
                .header("Range", "bytes=0-3")
                .POST(HttpRequest.BodyPublishers.ofString(reference.toString())).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(416, range.statusCode());
        }
    }

    @Test
    @TestSecurity(user = "alice")
    void rejectsOversizeAndDisallowedMediaWithoutPublishingAnObject() throws Exception {
        long before = completedObjects();
        byte[] oversized = new byte[1025];
        Arrays.fill(oversized, (byte) 'x');
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> tooLarge = client.send(uploadRequest(oversized, "application/pdf"),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(413, tooLarge.statusCode());

            HttpResponse<String> disallowed = client.send(uploadRequest("content".getBytes(StandardCharsets.UTF_8),
                "text/plain"), HttpResponse.BodyHandlers.ofString());
            assertEquals(415, disallowed.statusCode());
        }
        assertEquals(before, completedObjects());
    }

    @Test
    void requiresAuthenticationBeforeUpload() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(uploadRequest(new byte[] {1}, "application/pdf"),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(401, response.statusCode());
        }
    }

    private HttpRequest uploadRequest(byte[] payload, String contentType) {
        String marker = "tpf-owned-payload-boundary";
        byte[] prefix = ("--" + marker + "\r\nContent-Disposition: form-data; name=\"file\"; "
            + "filename=\"invoice.pdf\"\r\nContent-Type: " + contentType + "\r\n\r\n")
            .getBytes(StandardCharsets.UTF_8);
        byte[] suffix = ("\r\n--" + marker + "--\r\n").getBytes(StandardCharsets.UTF_8);
        return ownedRequest("/tpf/payloads/invoice/upload")
            .header("Content-Type", "multipart/form-data; boundary=" + marker)
            .POST(HttpRequest.BodyPublishers.ofByteArrays(List.of(prefix, payload, suffix)))
            .build();
    }

    private static long completedObjects() throws Exception {
        Path root = Path.of("target/owned-payload-http");
        if (!Files.exists(root)) {
            return 0;
        }
        try (var paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private HttpRequest.Builder ownedRequest(String path) {
        return HttpRequest.newBuilder(base.resolve(path))
            .header("X-Tenant-Id", "tenant-a")
            .header("X-Scope-Id", "invoice-1");
    }
}
