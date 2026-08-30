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

package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.StandardLocation;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineCompactFieldProcessorIntegrationTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void compactAndVerboseFieldsProduceIdenticalProcessorContracts(boolean includeAspects) throws IOException {
        JsonObject compact = compile("compact-" + includeAspects, compactFields(), includeAspects);
        JsonObject verbose = compile("verbose-" + includeAspects, verboseFields(), includeAspects);

        assertEquals(verbose, compact);
        assertEquals(verbose.get("contractHash"), compact.get("contractHash"));
    }

    private JsonObject compile(String fixtureName, String fields, boolean includeAspects) throws IOException {
        Path projectRoot = tempDir.resolve(fixtureName);
        Files.createDirectories(projectRoot);
        Files.writeString(projectRoot.resolve("pom.xml"),
            "<project><modelVersion>4.0.0</modelVersion><groupId>x</groupId>"
                + "<artifactId>x</artifactId><version>1</version></project>");
        Path generatedSourcesDir = projectRoot.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);
        Path config = projectRoot.resolve("pipeline.yaml");
        Files.writeString(config, """
            version: 3
            appName: Compact Fields
            basePackage: com.example.fields
            transport: LOCAL
            platform: COMPUTE
            contract: { input: Customer, output: Customer }
            types:
              Customer:
                fields:
            %s
            %s
            steps:
              - name: Process customer
                service: com.example.fields.CustomerService
                cardinality: ONE_TO_ONE
                input: Customer
                output: Customer
                java:
                  input: com.example.fields.Customer
                  output: com.example.fields.Customer
            """.formatted(fields, aspects(includeAspects)));

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.config=" + config.toString().replace('\\', '/'),
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir.toString().replace('\\', '/'),
                "-Apipeline.transport=LOCAL",
                "-Apipeline.platform=COMPUTE")
            .compile(
                JavaFileObjects.forSourceString("com.example.fields.Customer", """
                    package com.example.fields;
                    public final class Customer { }
                    """),
                JavaFileObjects.forSourceString("com.example.fields.CustomerService", """
                    package com.example.fields;
                    import io.smallrye.mutiny.Uni;
                    import org.pipelineframework.service.ReactiveService;
                    public final class CustomerService implements ReactiveService<Customer, Customer> {
                      public Uni<Customer> process(Customer input) { return Uni.createFrom().item(input); }
                    }
                    """));

        assertThat(compilation).succeeded();
        String contract = compilation.generatedFile(
            StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", "pipeline-contract.json")
            .orElseThrow().getCharContent(true).toString();
        return JsonParser.parseString(contract).getAsJsonObject();
    }

    private static String compactFields() {
        return """
                  - [name, string]
                  - [note?, string]
                  - [middleName, string?]
                  - [alias?, string?]
            """;
    }

    private static String verboseFields() {
        return """
                  - { name: name, type: string, presence: required, nullability: non_null }
                  - { name: note, type: string, presence: optional, nullability: non_null }
                  - { name: middleName, type: string, presence: required, nullability: nullable }
                  - { name: alias, type: string, presence: optional, nullability: nullable }
            """;
    }

    private static String aspects(boolean includeAspects) {
        if (!includeAspects) {
            return "";
        }
        return """
            aspects:
              audit:
                enabled: false
                scope: GLOBAL
                position: BEFORE_STEP
                order: 0
            """;
    }
}
