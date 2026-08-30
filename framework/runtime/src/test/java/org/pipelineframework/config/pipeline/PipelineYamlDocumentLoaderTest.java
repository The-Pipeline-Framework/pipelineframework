/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.config.pipeline;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.yaml.snakeyaml.error.YAMLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineYamlDocumentLoaderTest {

    private final PipelineYamlDocumentLoader loader = new PipelineYamlDocumentLoader();

    @Test
    void loadsEveryCompactV3FieldMarkerCombination() {
        Map<?, ?> root = root(loader.load(new StringReader("""
            version: 3
            types:
              Customer:
                fields:
                  - [name, string]
                  - [note?, string]
                  - [middleName, string?]
                  - [alias?, string?]
            """)));

        Map<?, ?> customer = root((Map<?, ?>) root.get("types")).get("Customer") instanceof Map<?, ?> value
            ? value
            : throwUnexpectedDocument();
        List<?> fields = (List<?>) customer.get("fields");

        assertEquals(List.of("name", "string"), fields.get(0));
        assertEquals(List.of("note?", "string"), fields.get(1));
        assertEquals(List.of("middleName", "string?"), fields.get(2));
        assertEquals(List.of("alias?", "string?"), fields.get(3));
    }

    @Test
    void loadsCompactMarkersForQuotedVersionThreeDocuments() {
        for (String version : List.of("\"3\"", "'3'")) {
            Map<?, ?> root = root(loader.load(new StringReader("""
                version: %s
                types:
                  Customer:
                    fields: [[note?, string?]]
                """.formatted(version))));
            Map<?, ?> customer = root((Map<?, ?>) root.get("types")).get("Customer") instanceof Map<?, ?> value
                ? value
                : throwUnexpectedDocument();

            assertEquals(List.of("note?", "string?"), ((List<?>) customer.get("fields")).getFirst());
        }
    }

    @Test
    void loadsUtf8FromPathAndInputStream(@TempDir Path tempDir) throws Exception {
        String source = "version: 3\nappName: Catálogo\ntypes: {}\n";
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, source, StandardCharsets.UTF_8);

        assertEquals("Catálogo", root(loader.load(config)).get("appName"));
        assertEquals("Catálogo", root(loader.load(
            new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)))).get("appName"));
    }

    @Test
    void doesNotNormalizeMarkersOutsideV3TypeFields() {
        assertThrows(YAMLException.class, () -> loader.load(new StringReader("""
            version: 3
            types:
              Customer:
                fields: [[name, string]]
            steps:
              - [note?, string]
            """)));
        assertThrows(YAMLException.class, () -> loader.load(new StringReader("""
            version: 2
            types:
              Customer:
                fields:
                  - [note?, string]
            """)));
        assertThrows(YAMLException.class, () -> loader.load(new StringReader("""
            version: 2
            metadata:
              version: 3
            types:
              Customer:
                fields:
                  - [note?, string]
            """)));
    }

    @Test
    void rejectsDuplicateKeysAndExcessiveAliases() {
        assertThrows(YAMLException.class, () -> loader.load(new StringReader("""
            version: 3
            appName: first
            appName: second
            """)));

        StringBuilder aliases = new StringBuilder("version: 3\nanchor: &shared [value]\naliases:\n");
        for (int index = 0; index < 51; index++) {
            aliases.append("  - *shared\n");
        }
        assertThrows(YAMLException.class, () -> loader.load(new StringReader(aliases.toString())));
    }

    @Test
    void rejectsOversizedInputWhileBuffering() {
        assertThrows(YAMLException.class, () -> loader.load(new StringReader("a".repeat(3_000_001))));
    }

    private static Map<?, ?> root(Object document) {
        if (document instanceof Map<?, ?> map) {
            return map;
        }
        return throwUnexpectedDocument();
    }

    private static <T> T throwUnexpectedDocument() {
        throw new AssertionError("Expected a YAML map document");
    }
}
