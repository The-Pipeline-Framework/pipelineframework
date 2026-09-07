package org.pipelineframework.search.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/** Launches the packaged REST JAR, independently of the provided cloud-function extensions. */
public final class PackagedSearchApplication implements QuarkusTestResourceLifecycleManager {
    private Optional<Process> process = Optional.empty();

    @Override
    public Map<String, String> start() {
        Path module = Path.of(System.getProperty("user.dir"));
        Path log = module.resolve("target/packaged-resource-it.log");
        Path jar = module.resolve("target/quarkus-app/quarkus-run.jar");
        String port = System.getProperty("quarkus.http.test-ssl-port", "8444");
        if (!Files.isRegularFile(jar)) {
            throw new IllegalStateException("Package the REST application before running its IT: " + jar);
        }
        boolean started = false;
        try {
            Process application = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-preview", "-Dquarkus.profile=test", "-Dquarkus.http.host=localhost",
                "-Dquarkus.http.ssl-port=" + port, "-Dquarkus.grpc.server.use-separate-server=false",
                "-jar", jar.toString())
                .directory(module.toFile()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
            process = Optional.of(application);
            long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
            while (application.isAlive() && System.nanoTime() < deadline) {
                if (Files.readString(log).contains("Listening on: https://localhost:" + port)) {
                    started = true;
                    // Failsafe supplies the test host before Quarkus chooses its launcher.
                    return Map.of();
                }
                Thread.sleep(100);
            }
            throw new IllegalStateException("Packaged REST application did not start; see " + log);
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot launch packaged REST application", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for packaged REST application", interrupted);
        } finally {
            // A failed resource startup is not guaranteed to receive a stop callback.
            if (!started) {
                stop();
            }
        }
    }

    @Override
    public void stop() {
        process.ifPresent(application -> {
            application.destroy();
            try {
                if (!application.waitFor(10, TimeUnit.SECONDS)) {
                    application.destroyForcibly();
                }
            } catch (InterruptedException interrupted) {
                application.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        });
        process = Optional.empty();
    }
}
