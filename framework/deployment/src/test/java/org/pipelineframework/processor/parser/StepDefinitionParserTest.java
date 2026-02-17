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

package org.pipelineframework.processor.parser;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.tools.Diagnostic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StepKind;

class StepDefinitionParserTest {

    @TempDir
    Path tempDir;

    @Test
    void rejectsStepWhenServiceAndDelegateAreBothProvided() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-step"
                service: "com.example.app.InternalService"
                delegate: "com.example.lib.ExternalService"
            """);

        assertTrue(steps.isEmpty());
    }

    @Test
    void ignoresExternalMapperForInternalStep() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-internal"
                service: "com.example.app.InternalService"
                externalMapper: "com.example.app.SomeMapper"
            """);

        assertEquals(1, steps.size());
        assertNull(steps.getFirst().externalMapper());
    }

    @Test
    void warnsAndIgnoresInputOutputForInternalStep() throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "internal-with-types"
                service: "com.example.app.InternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
            """);

        List<String> diagnostics = new ArrayList<>();
        StepDefinitionParser parser = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message));
        List<StepDefinition> steps = parser.parseStepDefinitions(file);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.INTERNAL, step.kind());
        assertNull(step.inputType());
        assertNull(step.outputType());
        String warningSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(warningSummary.contains(Diagnostic.Kind.WARNING.name()));
        assertTrue(warningSummary.contains("Ignoring 'input'/'output' on internal step"));
    }

    @Test
    void rejectsDelegatedStepWhenOnlyOneTypeIsProvided() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-delegated"
                delegate: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
            """);

        assertTrue(steps.isEmpty());
    }

    @Test
    void acceptsDelegatedStepWithOptionalExternalMapper() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "good-delegated"
                delegate: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                externalMapper: "com.example.app.ExternalMapperImpl"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals("good-delegated", step.name());
        assertEquals(StepKind.DELEGATED, step.kind());
        assertEquals("com.example.lib.ExternalService", step.executionClass().canonicalName());
        assertEquals("com.example.app.ExternalMapperImpl", step.externalMapper().canonicalName());
    }

    @Test
    void reportsWarningForUnsupportedStepKeys() throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "step-with-extra"
                service: "com.example.app.InternalService"
                unexpectedField: "value"
            """);

        List<String> diagnostics = new ArrayList<>();
        StepDefinitionParser parser = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message));
        List<StepDefinition> steps = parser.parseStepDefinitions(file);

        assertEquals(1, steps.size());
        String warningSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(warningSummary.contains(Diagnostic.Kind.WARNING.name()));
        assertTrue(warningSummary.contains("unsupported keys"));
        assertTrue(warningSummary.contains("unexpectedField"));
    }

    private List<StepDefinition> parse(String yaml) throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, yaml);
        return new StepDefinitionParser().parseStepDefinitions(file);
    }
}
