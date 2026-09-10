package org.pipelineframework.connector.openapi.maven;

import java.io.File;
import java.io.IOException;
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
        HttpResponse<byte[]> response = client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30))
            .header("Accept", "application/yaml, application/json").GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200 || response.body().length > OpenApiAcquisition.MAX_DOCUMENT_BYTES) {
            throw new IllegalArgumentException("OpenAPI acquisition failed or exceeded the document limit: " + uri);
        }
        return response.body();
    }
}
