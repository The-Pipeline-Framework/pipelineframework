package org.pipelineframework.connector;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorProviderExternalJarDiscoveryTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void discoversAnExternalProviderAndItsStaticMetadataWithoutConstructingItForMetadata() throws Exception {
        Path providerJar = compileExternalProviderJar();
        System.clearProperty("tpf.connector.external-provider-created");
        try (URLClassLoader loader = new URLClassLoader(new URL[] {providerJar.toUri().toURL()}, getClass().getClassLoader())) {
            ConnectorProviderManifestCatalog metadata = ConnectorProviderManifestLoader.load(loader);

            assertEquals(1, metadata.providers().size());
            assertEquals("external.fake", metadata.providers().getFirst().provider().id().value());
            assertEquals("echo", metadata.providers().getFirst().operations().getFirst().id());
            assertFalse(Boolean.getBoolean("tpf.connector.external-provider-created"));

            ConnectorRegistry registry = ConnectorRegistry.discover(loader);
            assertEquals(1, registry.providers().size());
            assertFalse(registry.operations().isEmpty());
            assertTrue(Boolean.getBoolean("tpf.connector.external-provider-created"));
            ConnectorProvider<?> runtimeProvider = registry.requireProvider(
                ConnectorProviderId.of("external.fake"), 1);
            ConnectorProviderArtifactDescriptor manifestProvider = metadata.providers().getFirst();
            assertEquals(manifestProvider.provider(), ConnectorDescriptors.provider(runtimeProvider));
            assertEquals(
                manifestProvider.operations(),
                runtimeProvider.operations().stream().map(ConnectorDescriptors::operation).toList());
        }
    }

    private Path compileExternalProviderJar() throws IOException {
        Path sourceRoot = temporaryDirectory.resolve("source");
        Path classesRoot = temporaryDirectory.resolve("classes");
        Path source = sourceRoot.resolve("external/fake/ExternalProvider.java");
        Files.createDirectories(source.getParent());
        Files.createDirectories(classesRoot);
        Files.writeString(source, externalProviderSource(), StandardCharsets.UTF_8);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("a JDK compiler is required for external provider conformance testing");
        }
        int result = compiler.run(
            null,
            null,
            null,
            "--release",
            "21",
            "-classpath",
            System.getProperty("java.class.path"),
            "-d",
            classesRoot.toString(),
            source.toString());
        if (result != 0) {
            throw new IllegalStateException("external provider fixture did not compile");
        }

        try (URLClassLoader buildLoader = new URLClassLoader(
            new URL[] {classesRoot.toUri().toURL()}, getClass().getClassLoader())) {
            ConnectorProvider<?> provider = (ConnectorProvider<?>) buildLoader
                .loadClass("external.fake.ExternalProvider").getConstructor().newInstance();
            ConnectorProviderArtifacts.write(classesRoot, List.of(provider));
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("external provider fixture could not be packaged", exception);
        }

        Path jar = temporaryDirectory.resolve("external-provider.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            addCompiledClasses(classesRoot, output);
        }
        System.clearProperty("tpf.connector.external-provider-created");
        return jar;
    }

    private static void addCompiledClasses(Path classesRoot, JarOutputStream output) throws IOException {
        try (var paths = Files.walk(classesRoot)) {
            for (Path path : paths.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList()) {
                String entryName = classesRoot.relativize(path).toString().replace('\\', '/');
                output.putNextEntry(new JarEntry(entryName));
                Files.copy(path, output);
                output.closeEntry();
            }
        }
    }

    private static String externalProviderSource() {
        return """
            package external.fake;

            import java.util.Collection;
            import java.util.List;
            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;
            import org.pipelineframework.connector.ConnectorOperation;
            import org.pipelineframework.connector.ConnectorConfigurationDocument;
            import org.pipelineframework.connector.ConnectorProvider;
            import org.pipelineframework.connector.ConnectorProviderId;
            import org.pipelineframework.connector.ConnectorProviderVersion;
            import org.pipelineframework.connector.QueryInvocation;
            import org.pipelineframework.connector.QueryCapabilities;
            import org.pipelineframework.connector.QueryOperation;
            import org.pipelineframework.connector.QueryOutcome;

            public final class ExternalProvider implements ConnectorProvider<Void> {
                public ExternalProvider() {
                    System.setProperty("tpf.connector.external-provider-created", "true");
                }

                @Override
                public ConnectorProviderId id() {
                    return ConnectorProviderId.of("external.fake");
                }

                @Override
                public ConnectorProviderVersion version() {
                    return new ConnectorProviderVersion(1, 0);
                }

                @Override
                public Collection<? extends ConnectorOperation> operations() {
                    return List.of(new Echo());
                }

                private static final class Echo implements QueryOperation<String, ConnectorConfigurationDocument, String> {
                    @Override
                    public String id() {
                        return "echo";
                    }

                    @Override
                    public QueryCapabilities capabilities() {
                        return QueryCapabilities.cacheable();
                    }

                    @Override
                    public CompletionStage<QueryOutcome<String>> query(
                        QueryInvocation<String, ConnectorConfigurationDocument, String> invocation
                    ) {
                        return CompletableFuture.completedFuture(new QueryOutcome.Found<>(invocation.input()));
                    }
                }
            }
            """;
    }
}
