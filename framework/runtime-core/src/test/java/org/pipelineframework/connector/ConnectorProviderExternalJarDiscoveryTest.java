package org.pipelineframework.connector;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
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
        }
    }

    private Path compileExternalProviderJar() throws IOException {
        Path sourceRoot = temporaryDirectory.resolve("source");
        Path classesRoot = temporaryDirectory.resolve("classes");
        Path source = sourceRoot.resolve("external/fake/ExternalProviderFactory.java");
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

        Path jar = temporaryDirectory.resolve("external-provider.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            addCompiledClasses(classesRoot, output);
            addEntry(output, "META-INF/services/org.pipelineframework.connector.ConnectorProviderFactory", "external.fake.ExternalProviderFactory\n");
            addEntry(output, "META-INF/pipeline/connector-providers.json", manifest());
        }
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

    private static void addEntry(JarOutputStream output, String name, String content) throws IOException {
        output.putNextEntry(new JarEntry(name));
        output.write(content.getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
    }

    private static String manifest() {
        return """
            {
              "schemaVersion": 1,
              "providers": [
                {
                  "id": "external.fake",
                  "version": {"major": 1, "minor": 0},
                  "operations": [{"id": "echo", "kind": "tpf:query", "majorVersion": 1}]
                }
              ]
            }
            """;
    }

    private static String externalProviderSource() {
        return """
            package external.fake;

            import java.util.Collection;
            import java.util.List;
            import java.util.concurrent.CompletionStage;
            import org.pipelineframework.connector.ConnectorCompletionStages;
            import org.pipelineframework.connector.ConnectorOperation;
            import org.pipelineframework.connector.ConnectorOperationDescriptor;
            import org.pipelineframework.connector.ConnectorOperationKind;
            import org.pipelineframework.connector.ConnectorProvider;
            import org.pipelineframework.connector.ConnectorProviderDescriptor;
            import org.pipelineframework.connector.ConnectorProviderFactory;
            import org.pipelineframework.connector.ConnectorProviderId;
            import org.pipelineframework.connector.ConnectorProviderVersion;
            import org.pipelineframework.connector.ConnectorRuntimeContext;

            public final class ExternalProviderFactory implements ConnectorProviderFactory {
                @Override
                public ConnectorProvider<?> create() {
                    System.setProperty("tpf.connector.external-provider-created", "true");
                    return new ExternalProvider();
                }

                private static final class ExternalProvider implements ConnectorProvider<Void> {
                    @Override
                    public ConnectorProviderDescriptor descriptor() {
                        return new ConnectorProviderDescriptor(ConnectorProviderId.of("external.fake"), new ConnectorProviderVersion(1, 0));
                    }

                    @Override
                    public Collection<? extends ConnectorOperation> operations() {
                        return List.of(() -> new ConnectorOperationDescriptor("echo", ConnectorOperationKind.QUERY, 1));
                    }

                    @Override
                    public CompletionStage<Void> start(ConnectorRuntimeContext context) {
                        return ConnectorCompletionStages.completed();
                    }

                    @Override
                    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
                        return ConnectorCompletionStages.completed();
                    }
                }
            }
            """;
    }
}
