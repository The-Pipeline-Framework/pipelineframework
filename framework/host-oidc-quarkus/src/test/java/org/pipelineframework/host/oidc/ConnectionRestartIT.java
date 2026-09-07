package org.pipelineframework.host.oidc;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.QuarkusProdModeTest;
import io.quarkus.test.common.QuarkusTestResource;
import java.net.CookieManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Packages a real Quarkus application and stops/restarts its JVM around the same encrypted JDBC file. */
@QuarkusTestResource(OidcFixture.class)
class ConnectionRestartIT {
    private static final String database = Path.of("target", "restart-" + UUID.randomUUID()).toAbsolutePath().toString();
    private static final int port = port();
    private static final URI base = URI.create("http://localhost:" + port + "/");

    @RegisterExtension static final QuarkusProdModeTest application = new QuarkusProdModeTest()
        .withApplicationRoot(archive -> {
            // Include production classes/resources, never the other test applications' CDI beans.
            Path root = Path.of("target/classes");
            try (var files = Files.walk(root)) {
                files.filter(Files::isRegularFile).forEach(file -> archive.addAsResource(file.toFile(), root.relativize(file).toString()));
            } catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
            archive.addClasses(RestartHost.class, RestartHost.Client.class, RestartHost.Mounted.class, RestartHost.Probe.class);
            try { archive.addClass(Class.forName(RestartHost.class.getName() + "$1")); }
            catch (ClassNotFoundException failure) { throw new IllegalStateException(failure); }
        })
        .overrideConfigKey("test.restart-host", "true")
        .setForcedDependencies(java.util.List.of(io.quarkus.maven.dependency.Dependency.of("com.h2database", "h2", org.h2.engine.Constants.VERSION)))
        .overrideConfigKey("quarkus.keycloak.devservices.enabled", "false")
        .setRuntimeProperties(Map.of("quarkus.http.port", Integer.toString(port),
            "test.jdbc.url", "jdbc:h2:file:" + database,
            "test.flow.uri", base.resolve("connections/proof/authorize").toString()))
        .setRun(true);

    @Test void sessionlessResolutionAndQuarkusRenewalSurviveProcessRestart() throws Exception {
        var cookies = new CookieManager();
        try (var browser = HttpClient.newBuilder().cookieHandler(cookies).followRedirects(HttpClient.Redirect.ALWAYS).build()) {
            var connected = browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect"))
                .header("Origin", "http://localhost:" + port).header("X-Test-Actor", "alice")
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, connected.statusCode());
            cookies.getCookieStore().removeAll();
        }
        try (var client = HttpClient.newHttpClient()) {
            assertEquals(204, client.send(HttpRequest.newBuilder(base.resolve("proof/expire"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding()).statusCode());
        }
        int renewals = OidcFixture.refreshes.get();
        application.stop();
        application.start();
        try (var client = HttpClient.newHttpClient()) {
            var response = client.send(HttpRequest.newBuilder(base.resolve("proof/me")).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("fixture-user"));
            assertEquals(renewals + 1, OidcFixture.refreshes.get());
        }
    }
    private static int port() {
        try (var socket = new java.net.ServerSocket(0)) { return socket.getLocalPort(); }
        catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
    }
}
