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

package org.pipelineframework.runtime.spring;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeSpringDependencyGuardTest {

    @Test
    void runtimeSpringHasNoQuarkusReferences() {
        assertNoForbiddenMainSourceToken("io.quarkus.");
    }

    @Test
    void runtimeSpringHasNoVertxReferences() {
        assertNoForbiddenMainSourceToken("io.vertx.");
    }

    @Test
    void runtimeSpringHasNoMutinyReferences() {
        assertNoForbiddenMainSourceToken("io.smallrye.mutiny");
    }

    @Test
    void runtimeSpringDoesNotDependOnQuarkusRuntimeArtifact() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"));

        assertFalse(pom.contains("<artifactId>pipelineframework</artifactId>"),
            "runtime-spring must depend on runtime-core, not the Quarkus runtime artifact");
        assertTrue(pom.contains("<artifactId>pipelineframework-runtime-core</artifactId>"));
    }

    private void assertNoForbiddenMainSourceToken(String forbiddenToken) {
        List<String> violations = collectViolations(forbiddenToken);
        assertTrue(
            violations.isEmpty(),
            "runtime-spring has forbidden dependency token '" + forbiddenToken + "':\n" + String.join("\n", violations));
    }

    private List<String> collectViolations(String token) {
        Path sourceRoot = Path.of("src/main/java");
        if (!Files.isDirectory(sourceRoot)) {
            return List.of("Unable to scan runtime-spring sources: missing directory " + sourceRoot);
        }

        List<String> violations = new ArrayList<>();
        try (var stream = Files.walk(sourceRoot)) {
            stream.filter(path -> path.toString().endsWith(".java"))
                .forEach(path -> collectFileViolations(path, token, violations));
        } catch (IOException e) {
            violations.add("Unable to scan runtime-spring sources: " + e.getMessage());
        }
        Collections.sort(violations);
        return violations;
    }

    private void collectFileViolations(Path path, String token, List<String> violations) {
        try {
            List<String> lines = Files.readAllLines(path);
            for (int lineNo = 1; lineNo <= lines.size(); lineNo++) {
                if (lines.get(lineNo - 1).contains(token)) {
                    violations.add(path + ":" + lineNo);
                }
            }
        } catch (IOException e) {
            violations.add("Unable to read " + path + ": " + e.getMessage());
        }
    }
}
