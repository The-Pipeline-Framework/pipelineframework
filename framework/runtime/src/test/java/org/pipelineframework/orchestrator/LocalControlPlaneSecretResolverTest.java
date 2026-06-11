package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class LocalControlPlaneSecretResolverTest {

    private final LocalControlPlaneSecretResolver resolver = new LocalControlPlaneSecretResolver();

    @BeforeEach
    @AfterEach
    void clearSystemProperties() {
        System.clearProperty("tpf.test.worker.secret");
        System.clearProperty("tpf.test.config.secret");
    }

    @Test
    void resolvesEnvironmentReference() {
        assumeTrue(System.getenv("PATH") != null && !System.getenv("PATH").isBlank());

        assertEquals(System.getenv("PATH"), resolver.resolve("env:PATH"));
    }

    @Test
    void resolvesSystemPropertyReference() {
        System.setProperty("tpf.test.worker.secret", "sys-secret");

        assertEquals("sys-secret", resolver.resolve("sys:tpf.test.worker.secret"));
    }

    @Test
    void resolvesConfigReferenceFromSystemPropertyConfigSource() {
        System.setProperty("tpf.test.config.secret", "config-secret");

        assertEquals("config-secret", resolver.resolve("config:tpf.test.config.secret"));
    }

    @Test
    void rejectsBlankReference() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> resolver.resolve(" "));

        assertEquals("Secret reference must not be blank", error.getMessage());
    }

    @Test
    void rejectsUnsupportedScheme() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> resolver.resolve("vault:path"));

        assertEquals("Unsupported secret reference scheme: vault:", error.getMessage());
    }

    @Test
    void rejectsUnresolvedReference() {
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> resolver.resolve("sys:tpf.test.worker.secret"));

        assertEquals("Secret reference could not be resolved: sys:tpf.test.worker.secret", error.getMessage());
    }

    @Test
    void rejectsReferenceWithoutKey() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> resolver.resolve("sys: "));

        assertEquals("Secret reference sys: requires a key", error.getMessage());
    }
}
