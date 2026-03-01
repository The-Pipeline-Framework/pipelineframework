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
import org.pipelineframework.processor.ir.MapperFallbackMode;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StepKind;

class StepDefinitionParserTest {

    @TempDir
    Path tempDir;

    @Test
    void rejectsStepWhenServiceAndOperatorAreBothProvided() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-step"
                service: "com.example.app.InternalService"
                operator: "com.example.lib.ExternalService"
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
                operatorMapper: "com.example.app.SomeMapper"
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
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
            """);

        assertTrue(steps.isEmpty());
    }

    @Test
    void acceptsDelegatedStepWithOptionalOperatorMapper() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "good-delegated"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                operatorMapper: "com.example.app.ExternalMapperImpl"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals("good-delegated", step.name());
        assertEquals(StepKind.DELEGATED, step.kind());
        assertEquals("com.example.lib.ExternalService", step.executionClass().canonicalName());
        assertEquals("com.example.app.ExternalMapperImpl", step.externalMapper().canonicalName());
        assertEquals(MapperFallbackMode.NONE, step.mapperFallback());
    }

    @Test
    void parsesDelegatedMapperFallbackJackson() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "fallback-step"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                mapperFallback: "JACKSON"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.DELEGATED, step.kind());
        assertEquals(MapperFallbackMode.JACKSON, step.mapperFallback());
    }

    @Test
    void acceptsLegacyDelegateAndExternalMapperAliases() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "legacy-delegated"
                delegate: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                externalMapper: "com.example.app.ExternalMapperImpl"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.DELEGATED, step.kind());
        assertEquals("com.example.lib.ExternalService", step.executionClass().canonicalName());
        assertEquals("com.example.app.ExternalMapperImpl", step.externalMapper().canonicalName());
    }

    @Test
    void acceptsDelegatedStepWithoutInputOutputForTypeInference() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "delegate-only"
                operator: "com.example.lib.ExternalService"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.DELEGATED, step.kind());
        assertNull(step.inputType());
        assertNull(step.outputType());
    }

    @Test
    void rejectsStepWhenOperatorAndDelegateAliasesAreBothProvided() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-alias-step"
                operator: "com.example.lib.ExternalService"
                delegate: "com.example.lib.ExternalService2"
            """);

        assertTrue(steps.isEmpty());
    }

    @Test
    void rejectsInvalidClassNames() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-class"
                service: "com..example.BadService"
            """);
        assertTrue(steps.isEmpty());

        List<StepDefinition> steps2 = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-class-2"
                service: ".com.example.BadService"
            """);
        assertTrue(steps2.isEmpty());
    }

    @Test
    void returnsEmptyWhenTemplatePathDoesNotExist() throws IOException {
        Path missing = tempDir.resolve("missing-pipeline.yaml");
        List<StepDefinition> steps = new StepDefinitionParser().parseStepDefinitions(missing);
        assertTrue(steps.isEmpty());
    }

    @Test
    void returnsEmptyWhenYamlHasNoStepsKey() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            transport: "GRPC"
            """);
        assertTrue(steps.isEmpty());
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

    @Test
    void ignoresMapperFallbackForInternalStep() throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "internal-with-fallback"
                service: "com.example.app.InternalService"
                mapperFallback: "JACKSON"
            """);

        List<String> diagnostics = new ArrayList<>();
        StepDefinitionParser parser = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message));
        List<StepDefinition> steps = parser.parseStepDefinitions(file);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.INTERNAL, step.kind());
        assertEquals(MapperFallbackMode.NONE, step.mapperFallback());
        String warningSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(warningSummary.contains(Diagnostic.Kind.WARNING.name()));
        assertTrue(warningSummary.contains("Ignoring 'mapperFallback' on internal step"));
    }

    @Test
    void defaultsMapperFallbackToNoneWhenNotSpecified() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "no-fallback"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
            """);

        assertEquals(1, steps.size());
        assertEquals(MapperFallbackMode.NONE, steps.getFirst().mapperFallback());
    }

    @Test
    void rejectsInvalidMapperFallbackValue() throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "invalid-fallback"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                mapperFallback: "INVALID_MODE"
            """);

        List<String> diagnostics = new ArrayList<>();
        StepDefinitionParser parser = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message));
        List<StepDefinition> steps = parser.parseStepDefinitions(file);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("invalid mapperFallback"));
        assertTrue(errorSummary.contains("INVALID_MODE"));
    }

    @Test
    void parsesMapperFallbackCaseInsensitive() throws IOException {
        List<StepDefinition> stepsLower = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "fallback-lower"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                mapperFallback: "jackson"
            """);

        assertEquals(1, stepsLower.size());
        assertEquals(MapperFallbackMode.JACKSON, stepsLower.getFirst().mapperFallback());

        List<StepDefinition> stepsMixed = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "fallback-mixed"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                mapperFallback: "JaCkSoN"
            """);

        assertEquals(1, stepsMixed.size());
        assertEquals(MapperFallbackMode.JACKSON, stepsMixed.getFirst().mapperFallback());
    }

    @Test
    void parsesMapperFallbackNone() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "fallback-none"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                mapperFallback: "NONE"
            """);

        assertEquals(1, steps.size());
        assertEquals(MapperFallbackMode.NONE, steps.getFirst().mapperFallback());
    }

    @Test
    void allowsBothOperatorMapperAndMapperFallback() throws IOException {
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "mapper-and-fallback"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                operatorMapper: "com.example.app.ExternalMapperImpl"
                mapperFallback: "JACKSON"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertNotNull(step.externalMapper());
        assertEquals(MapperFallbackMode.JACKSON, step.mapperFallback());
    }

    private List<StepDefinition> parse(String yaml) throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, yaml);
        return new StepDefinitionParser().parseStepDefinitions(file);
    }
}