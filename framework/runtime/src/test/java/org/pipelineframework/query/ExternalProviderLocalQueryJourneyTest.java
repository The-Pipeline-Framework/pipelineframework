package org.pipelineframework.query;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderDiscovery;
import org.pipelineframework.connector.ConnectorProviderArtifacts;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExternalProviderLocalQueryJourneyTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void localGeneratedShapeReachesExternalProviderThroughTheBindingRegistry() throws Exception {
        Path providerJar = compileExternalProviderJar();
        try (URLClassLoader loader = new URLClassLoader(
            new URL[] { providerJar.toUri().toURL() }, getClass().getClassLoader())) {
            ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
                ConnectorProviderId.of("external.query"), "echo", ConnectorOperationKind.QUERY, 1);
            QueryCapabilities capabilities = ConnectorProviderManifestLoader.load(loader)
                .requireQueryCapabilities(identity, 1);
            ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
                List.of(new ConnectorBindingDefinition(
                    ConnectorBindingName.of("external"),
                    identity.providerId(),
                    1,
                    ConnectorConfigurationDocument.empty())),
                ConnectorProviderDiscovery.discover(loader));
            try {
                QueryStepDescriptor descriptor = QueryStepDescriptor.nativeQuery(
                    "ExternalEcho",
                    String.class.getName(),
                    String.class.getName(),
                    "ONE_TO_ONE",
                    new NativeQuerySelector(ConnectorBindingName.of("external"), identity, 1),
                    Map.of("prefix", "external:"),
                    capabilities,
                    Optional.empty());
                LocalGeneratedQueryStep step = new LocalGeneratedQueryStep(
                    new QueryStepSupport(List.of(), List.of(), registry), descriptor, identity, capabilities);

                assertEquals("external:hello", step.applyOneToOne("hello")
                    .await().atMost(Duration.ofSeconds(2)));
            } finally {
                registry.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
            }
        }
    }

    private Path compileExternalProviderJar() throws IOException {
        Path sourceRoot = temporaryDirectory.resolve("source");
        Path classesRoot = temporaryDirectory.resolve("classes");
        Path source = sourceRoot.resolve("external/query/ExternalQueryProvider.java");
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
            throw new IllegalStateException("external Query provider fixture did not compile");
        }

        try (URLClassLoader buildLoader = new URLClassLoader(
            new URL[] {classesRoot.toUri().toURL()}, getClass().getClassLoader())) {
            ConnectorProvider<?> provider = (ConnectorProvider<?>) buildLoader
                .loadClass("external.query.ExternalQueryProvider").getConstructor().newInstance();
            ConnectorProviderArtifacts.write(classesRoot, List.of(provider));
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("external Query provider fixture could not be packaged", exception);
        }

        Path jar = temporaryDirectory.resolve("external-query-provider.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            try (var paths = Files.walk(classesRoot)) {
                for (Path path : paths.filter(Files::isRegularFile).sorted(Comparator.naturalOrder()).toList()) {
                    String entryName = classesRoot.relativize(path).toString().replace('\\', '/');
                    output.putNextEntry(new JarEntry(entryName));
                    Files.copy(path, output);
                    output.closeEntry();
                }
            }
        }
        return jar;
    }

    private static String externalProviderSource() {
        return """
            package external.query;

            import java.util.Collection;
            import java.util.List;
            import java.util.Optional;
            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;
            import org.pipelineframework.connector.*;

            public final class ExternalQueryProvider implements ConnectorProvider<Void> {
                @Override
                public ConnectorProviderId id() {
                    return ConnectorProviderId.of("external.query");
                }

                @Override
                public ConnectorProviderVersion version() {
                    return new ConnectorProviderVersion(1, 0);
                }

                @Override
                public Collection<? extends ConnectorOperation> operations() {
                    return List.of(new EchoOperation());
                }

                public record EchoConfig(String prefix) {}

                private static final class EchoOperation implements QueryOperation<String, EchoConfig, String> {
                    private static final ConnectorConfigSchema<EchoConfig> SCHEMA =
                        ConnectorConfigSchema.record(EchoConfig.class, "external.query.echo", 1);

                    @Override
                    public String id() {
                        return "echo";
                    }

                    @Override
                    public QueryCapabilities capabilities() {
                        return QueryCapabilities.cacheable();
                    }

                    @Override
                    public Optional<ConnectorConfigSchema<EchoConfig>> configurationSchema() {
                        return Optional.of(SCHEMA);
                    }

                    @Override
                    public CompletionStage<QueryOutcome<String>> query(QueryInvocation<String, EchoConfig, String> invocation) {
                        return CompletableFuture.completedFuture(
                            new QueryOutcome.Found<>(invocation.configuration().prefix() + invocation.input()));
                    }
                }
            }
            """;
    }

    private static final class LocalGeneratedQueryStep extends ConfigurableStep
        implements StepOneToOne<String, String>, ProviderQueryStep {
        private final QueryStepSupport support;
        private final QueryStepDescriptor descriptor;
        private final QueryCacheRequirements requirements;

        private LocalGeneratedQueryStep(
            QueryStepSupport support,
            QueryStepDescriptor descriptor,
            ConnectorOperationIdentity identity,
            QueryCapabilities capabilities
        ) {
            this.support = support;
            this.descriptor = descriptor;
            this.requirements = new QueryCacheRequirements(identity, 1, capabilities, Optional.empty());
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return support.queryOneToOne(descriptor, input, String.class);
        }

        @Override
        public QueryCacheRequirements queryCacheRequirements() {
            return requirements;
        }
    }
}
