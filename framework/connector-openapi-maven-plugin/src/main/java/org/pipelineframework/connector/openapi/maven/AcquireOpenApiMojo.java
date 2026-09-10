package org.pipelineframework.connector.openapi.maven;

import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/** The only network-capable OpenAPI goal; vendors an explicitly selected, bounded HTTPS contract closure. */
@Mojo(name = "acquire", requiresProject = true, threadSafe = false)
public final class AcquireOpenApiMojo extends org.apache.maven.plugin.AbstractMojo {
    @Parameter(property = "openapi.source", required = true)
    private String source;

    @Parameter(property = "openapi.snapshot", required = true)
    private File snapshot;

    /** Additional exact HTTPS origins from which referenced documents may be acquired. */
    @Parameter(property = "openapi.allowedOrigins")
    private List<String> allowedOrigins = List.of();

    @Override
    public void execute() throws MojoExecutionException {
        try {
            HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(20)).build();
            OpenApiAcquisition.Acquisition acquisition = OpenApiAcquisition.acquire(URI.create(source), snapshot.toPath(),
                Set.copyOf(allowedOrigins), uri -> fetch(client, uri));
            getLog().info("Acquired " + acquisition.documents().size() + " OpenAPI document(s); original closure SHA-256 "
                + acquisition.originalClosureSha256());
        } catch (IOException | InterruptedException | RuntimeException failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new MojoExecutionException("Unable to acquire OpenAPI contract snapshot", failure);
        }
    }

    private static byte[] fetch(HttpClient client, URI uri) throws IOException, InterruptedException {
        HttpResponse<InputStream> response = client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30))
            .header("Accept", "application/yaml, application/json").GET().build(), HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream body = response.body()) {
            if (response.statusCode() != 200
                    || response.headers().firstValueAsLong("Content-Length").stream()
                        .anyMatch(length -> length > OpenApiAcquisition.MAX_DOCUMENT_BYTES)) {
                throw new IllegalArgumentException("OpenAPI acquisition failed or exceeded the document limit: " + uri);
            }
            return readBounded(body, uri);
        }
    }

    static byte[] readBounded(InputStream body, URI uri) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int total = 0;
            int read;
            while ((read = body.read(buffer)) >= 0) {
                total += read;
                if (total > OpenApiAcquisition.MAX_DOCUMENT_BYTES) {
                    throw new IllegalArgumentException(
                        "OpenAPI acquisition failed or exceeded the document limit: " + uri);
                }
                bytes.write(buffer, 0, read);
            }
            return bytes.toByteArray();
        }
    }
}
