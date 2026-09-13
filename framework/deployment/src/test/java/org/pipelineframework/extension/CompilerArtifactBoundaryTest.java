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

package org.pipelineframework.extension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompilerArtifactBoundaryTest {

    @Test
    void deploymentConsumesCompilerOnlyForConformanceTests()
        throws IOException, ParserConfigurationException, SAXException {
        var document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(Path.of("pom.xml").toFile());
        List<Element> compilerDependencies = elements(document.getElementsByTagName("dependency")).stream()
            .filter(dependency -> "org.pipelineframework".equals(childText(dependency, "groupId")))
            .filter(dependency -> "pipelineframework-compiler".equals(childText(dependency, "artifactId")))
            .toList();

        assertEquals(1, compilerDependencies.size(), "deployment must declare one compiler dependency");
        assertEquals("test", childText(compilerDependencies.getFirst(), "scope"),
            "production Quarkus integration must not depend on compiler implementation");
    }

    @Test
    void deploymentSourcesDoNotReferenceCompilerImplementationTypes() throws IOException {
        List<String> forbiddenReferences = List.of(
            "org.pipelineframework.processor.PipelineStepProcessor",
            "org.pipelineframework.processor.phase.",
            "org.pipelineframework.processor.renderer.",
            "org.pipelineframework.proto.");

        try (var sources = Files.walk(Path.of("src/main/java"))) {
            List<Path> violations = sources
                .filter(path -> path.toString().endsWith(".java"))
                .filter(path -> containsAny(path, forbiddenReferences))
                .toList();
            assertTrue(violations.isEmpty(),
                "deployment production sources reference compiler implementation types: " + violations);
        }
    }

    private static boolean containsAny(Path source, List<String> forbiddenReferences) {
        try {
            String contents = Files.readString(source);
            return forbiddenReferences.stream().anyMatch(contents::contains);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to inspect " + source, e);
        }
    }

    private static List<Element> elements(NodeList nodes) {
        return IntStream.range(0, nodes.getLength())
            .mapToObj(nodes::item)
            .filter(Element.class::isInstance)
            .map(Element.class::cast)
            .toList();
    }

    private static String childText(Element parent, String name) {
        return Optional.ofNullable(parent.getElementsByTagName(name).item(0))
            .orElseThrow(() -> new IllegalStateException("Missing " + name + " in dependency"))
            .getTextContent()
            .trim();
    }
}
