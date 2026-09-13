/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import javax.annotation.processing.Processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompilerDependencyGuardTest {

    private static final List<String> FORBIDDEN_PACKAGES = List.of(
        "io.quarkus.",
        "io.smallrye.jandex.",
        "org.jboss.jandex.",
        "org.pipelineframework.extension.");

    @Test
    void compilerSourcesDoNotImportQuarkusOrJandex() {
        FORBIDDEN_PACKAGES.forEach(this::assertNoForbiddenImport);
    }

    @Test
    void forbiddenPackageGuardRecognizesRegularAndStaticImports(@TempDir Path tempDir) throws IOException {
        for (String forbiddenPackage : FORBIDDEN_PACKAGES) {
            Path source = tempDir.resolve(forbiddenPackage.replace('.', '-') + "Example.java");
            Files.writeString(source, """
                import %1$sType;
                import static %1$sType.member;
                """.formatted(forbiddenPackage));

            List<String> violations = new ArrayList<>();
            collectFileViolations(source, forbiddenPackage, violations);

            assertEquals(2, violations.size(), "both import forms must be forbidden for " + forbiddenPackage);
        }
    }

    @Test
    void compilerDoesNotDependOnDeploymentOrRuntimeImplementation() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"));

        assertFalse(pom.contains("<artifactId>pipelineframework-deployment</artifactId>"),
            "compiler must not depend on the Quarkus deployment integration");
        assertFalse(pom.contains("<artifactId>pipelineframework</artifactId>"),
            "compiler must not depend on the Quarkus runtime implementation");
        assertTrue(pom.contains("<artifactId>pipelineframework-semantic-model</artifactId>"),
            "compiler must depend on the shared semantic model");
    }

    @Test
    void compilerPublishesALoadableJsr269Processor() {
        Processor processor = ServiceLoader.load(Processor.class).stream()
            .filter(provider -> provider.type().equals(PipelineStepProcessor.class))
            .findFirst()
            .orElseThrow(() -> new AssertionError("compiler must publish PipelineStepProcessor as a JSR-269 service"))
            .get();

        assertInstanceOf(PipelineStepProcessor.class, processor);
    }

    private void assertNoForbiddenImport(String forbiddenPackage) {
        List<String> violations = collectViolations(forbiddenPackage);
        assertTrue(
            violations.isEmpty(),
            "compiler has forbidden import '" + forbiddenPackage + "':\n" + String.join("\n", violations));
    }

    private List<String> collectViolations(String forbiddenPackage) {
        Path sourceRoot = Path.of("src/main/java");
        if (!Files.isDirectory(sourceRoot)) {
            return List.of("Unable to scan compiler sources: missing directory " + sourceRoot);
        }

        List<String> violations = new ArrayList<>();
        try (var stream = Files.walk(sourceRoot)) {
            stream.filter(path -> path.toString().endsWith(".java"))
                .forEach(path -> collectFileViolations(path, forbiddenPackage, violations));
        } catch (IOException e) {
            violations.add("Unable to scan compiler sources: " + e.getMessage());
        }
        Collections.sort(violations);
        return violations;
    }

    private void collectFileViolations(Path path, String forbiddenPackage, List<String> violations) {
        try {
            List<String> lines = Files.readAllLines(path);
            for (int lineNo = 1; lineNo <= lines.size(); lineNo++) {
                String line = lines.get(lineNo - 1).stripLeading();
                if (line.startsWith("import " + forbiddenPackage)
                    || line.startsWith("import static " + forbiddenPackage)) {
                    violations.add(path + ":" + lineNo);
                }
            }
        } catch (IOException e) {
            violations.add("Unable to read " + path + ": " + e.getMessage());
        }
    }
}
