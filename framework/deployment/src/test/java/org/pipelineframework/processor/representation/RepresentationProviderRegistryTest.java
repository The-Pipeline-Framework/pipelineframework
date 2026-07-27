package org.pipelineframework.processor.representation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.representation.spi.ArtifactDescription;
import org.pipelineframework.representation.spi.ArtifactKind;
import org.pipelineframework.representation.spi.ArtifactPhase;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderMetadata;
import org.pipelineframework.representation.spi.ProviderConfiguration;
import org.pipelineframework.representation.spi.RepresentationProvider;
import org.pipelineframework.representation.spi.RepresentationScope;

class RepresentationProviderRegistryTest {
    private static final CanonicalType PAYMENT = new CanonicalType("Payment", "example.Payment", CanonicalTypeShape.RECORD);

    @Test
    void discoversFixtureFromItsSeparateJar() throws Exception {
        Path fixtureTarget = Path.of(System.getProperty("user.dir"))
            .resolve("../representation-provider-fixture/target").normalize();
        Path fixtureJar;
        try (var files = Files.list(fixtureTarget)) {
            fixtureJar = files.filter(path -> path.getFileName().toString().endsWith("-discovery.jar"))
                .findFirst().orElseThrow(() -> new IllegalStateException("Missing packaged external provider fixture JAR."));
        }
        assertTrue(Files.isRegularFile(fixtureJar));
        try (FixtureJarClassLoader loader = new FixtureJarClassLoader(fixtureJar.toUri().toURL())) {
            RepresentationProviderRegistry registry = RepresentationProviderRegistry.discover(loader);
            assertTrue(registry.providers().stream().map(provider -> provider.metadata().key())
                .anyMatch("external-fixture"::equals));
        }
    }

    @Test
    void missingProviderExplainsProcessorHostVisibility() {
        var diagnostic = RepresentationProviderRegistry.of(List.of())
            .validate(List.of(new ProviderConfiguration(RepresentationScope.TYPE, "missing", java.util.Map.of())))
            .getFirst();

        assertEquals("provider.absent", diagnostic.code());
        assertEquals("Representation provider 'missing' is not available to the annotation-processor host for TYPE "
                + "configuration. Application classpath visibility does not register a provider; add its JAR to the "
                + "annotation processor path.", diagnostic.message());
    }

    @Test
    void ordersProvidersByDeclaredProviderDependencyRatherThanLibraryOrder() {
        RepresentationProviderRegistry registry = RepresentationProviderRegistry.of(List.of(
            provider("consumer", Set.of("base")), provider("base", Set.of())));

        assertEquals(List.of("base", "consumer"), registry.providers().stream()
            .map(provider -> provider.metadata().key()).toList());
    }

    @Test
    void rejectsDuplicateAndCyclicProviderKeysDeterministically() {
        assertEquals("Duplicate representation provider key 'same'.", assertThrows(IllegalStateException.class,
            () -> RepresentationProviderRegistry.of(List.of(provider("same", Set.of()), provider("same", Set.of())))).getMessage());
        assertTrue(assertThrows(IllegalStateException.class, () -> RepresentationProviderRegistry.of(List.of(
            provider("a", Set.of("b")), provider("b", Set.of("a"))))).getMessage()
            .contains("Representation provider dependency cycle"));
    }

    @Test
    void resolvesZeroOneAndMultipleBoundaryClaimsDeterministically() {
        BoundaryRequest request = new BoundaryRequest("Read", "example.Reader", PAYMENT, PAYMENT, "EXPANSION", Set.of(), java.util.Map.of());
        assertTrue(RepresentationProviderRegistry.of(List.of(provider("none", Set.of()))).resolveClaim(request).isEmpty());
        assertEquals(Optional.of("one"), RepresentationProviderRegistry.of(List.of(claimingProvider("one")))
            .resolveClaim(request).map(BoundaryClaim::providerKey));
        assertEquals("Representation boundary 'Read' has multiple provider claimants: [alpha, zeta]",
            assertThrows(IllegalStateException.class, () -> RepresentationProviderRegistry.of(List.of(
                claimingProvider("zeta"), claimingProvider("alpha"))).resolveClaim(request)).getMessage());
    }

    @Test
    void hostOrdersAndWritesProviderArtifactsAndRejectsConflicts(@TempDir Path root) throws Exception {
        ProviderArtifactWriter writer = new ProviderArtifactWriter();
        List<Path> written = writer.write(root, List.of(
            artifact("zeta", ArtifactPhase.SOURCE, "zeta/Z.java", "Z"),
            artifact("alpha", ArtifactPhase.PRE_MODEL, "alpha/A.java", "A")));
        assertEquals(List.of("alpha/A.java", "zeta/Z.java"), written.stream()
            .map(path -> root.relativize(path).toString()).toList());
        assertEquals("A", Files.readString(root.resolve("alpha/A.java")));
        assertTrue(assertThrows(IllegalStateException.class, () -> writer.write(root, List.of(
            artifact("alpha", ArtifactPhase.SOURCE, "same.java", "A"),
            artifact("zeta", ArtifactPhase.SOURCE, "same.java", "Z")))).getMessage()
            .contains("Representation artifact conflict at 'same.java'"));
    }

    private static ArtifactDescription artifact(String provider, ArtifactPhase phase, String path, String content) {
        return new ArtifactDescription(provider, phase, ArtifactKind.JAVA_SOURCE, path, content, 0);
    }

    private static RepresentationProvider provider(String key, Set<String> dependencies) {
        return () -> new ProviderMetadata(key, dependencies, Set.of());
    }

    private static RepresentationProvider claimingProvider(String key) {
        return new RepresentationProvider() {
            @Override public ProviderMetadata metadata() { return new ProviderMetadata(key, Set.of(), Set.of()); }
            @Override public Optional<BoundaryClaim> claim(BoundaryRequest request) {
                return Optional.of(new BoundaryClaim(key, request.stepName(), "example." + key));
            }
        };
    }

    /** Isolates service descriptors to the packaged fixture JAR even though Maven also exposes its test dependency. */
    private static final class FixtureJarClassLoader extends URLClassLoader {
        private static final String SERVICE = "META-INF/services/" + RepresentationProvider.class.getName();

        private FixtureJarClassLoader(URL fixtureJar) {
            super(new URL[] { fixtureJar }, RepresentationProvider.class.getClassLoader());
        }

        @Override
        public java.util.Enumeration<URL> getResources(String name) throws java.io.IOException {
            if (SERVICE.equals(name)) {
                URL resource = findResource(name);
                return resource == null ? Collections.emptyEnumeration() : Collections.enumeration(List.of(resource));
            }
            return super.getResources(name);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.equals("org.pipelineframework.fixture.ExternalFixtureRepresentationProvider")) {
                Class<?> type = findLoadedClass(name);
                if (type == null) {
                    type = findClass(name);
                }
                if (resolve) {
                    resolveClass(type);
                }
                return type;
            }
            return super.loadClass(name, resolve);
        }
    }
}
