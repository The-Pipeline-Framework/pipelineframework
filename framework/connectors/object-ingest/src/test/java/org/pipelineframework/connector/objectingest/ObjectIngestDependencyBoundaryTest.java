package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectIngestDependencyBoundaryTest {

    @Test
    void connectorConsumesContractsWithoutTheRuntimeImplementation() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"));

        assertFalse(pom.contains("<artifactId>pipelineframework</artifactId>"));
        assertTrue(pom.contains("<artifactId>pipelineframework-semantic-model</artifactId>"));
        assertTrue(pom.contains("<artifactId>pipelineframework-runtime-core</artifactId>"));
        assertTrue(pom.contains("<artifactId>pipelineframework-runtime-api</artifactId>"));
        assertTrue(pom.contains("<artifactId>jakarta.enterprise.cdi-api</artifactId>"));
        assertFalse(pom.contains("<groupId>io.quarkus</groupId>"));
    }
}
