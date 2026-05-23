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

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.MapperFallbackMode;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StepKind;
import org.pipelineframework.processor.ir.StreamingShape;

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
    void rejectsOperatorMappersForInternalStep() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-internal"
                service: "com.example.app.InternalService"
                operatorMapper: "com.example.app.SomeMapper"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()), errorSummary);
    }

    @Test
    void parsesYamlOwnedTypesAndMappersForInternalStep() throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "internal-with-types"
                service: "com.example.app.InternalService"
                input: "com.example.app.InputType"
                inboundMapper: "com.example.app.InputMapper"
                output: "com.example.app.OutputType"
                outboundMapper: "com.example.app.OutputMapper"
            """);

        List<String> diagnostics = new ArrayList<>();
        StepDefinitionParser parser = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message));
        List<StepDefinition> steps = parser.parseStepDefinitions(file);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.INTERNAL, step.kind());
        assertEquals(ClassName.get("com.example.app", "InputType"), step.inputType());
        assertEquals(ClassName.get("com.example.app", "OutputType"), step.outputType());
        assertEquals(ClassName.get("com.example.app", "InputMapper"), step.inboundMapper());
        assertEquals(ClassName.get("com.example.app", "OutputMapper"), step.outboundMapper());
        String diagnosticSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertFalse(diagnosticSummary.contains(Diagnostic.Kind.ERROR.name()), diagnosticSummary);
        assertFalse(diagnosticSummary.contains(Diagnostic.Kind.WARNING.name()), diagnosticSummary);
    }

    @Test
    void parsesAwaitStepDefinition() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Fraud Check"
                kind: "await"
                cardinality: "ONE_TO_ONE"
                input: "com.example.FraudCheckRequest"
                output: "com.example.FraudCheckDecision"
                timeout: "PT10M"
                idempotencyKeyFields: ["orderId"]
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "webhook"
                    request:
                      url: "https://partner.example/check"
            """, diagnostics);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.AWAIT, step.kind());
        assertNull(step.executionClass());
        assertEquals(ClassName.get("com.example", "FraudCheckRequest"), step.inputType());
        assertEquals(ClassName.get("com.example", "FraudCheckDecision"), step.outputType());
        assertEquals("PT10M", step.timeout());
        assertEquals(List.of("orderId"), step.idempotencyKeyFields());
        assertEquals("webhook", ((java.util.Map<?, ?>) step.awaitConfig().get("transport")).get("type"));
        assertEquals("interactionId", ((java.util.Map<?, ?>) step.awaitConfig().get("correlation")).get("strategy"));
        assertEquals("https://partner.example/check",
            ((java.util.Map<?, ?>) ((java.util.Map<?, ?>) step.awaitConfig().get("transport")).get("request")).get("url"));
        assertTrue(diagnostics.stream().noneMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())));
    }

    @Test
    void parsesManyToManyAwaitStepWithPerItemDispatch() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Await Payment Provider"
                kind: "await"
                cardinality: "MANY_TO_MANY"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  dispatch:
                    mode: "per-item"
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "kafka"
                    request:
                      topic: "payment.requests"
                      key: "correlationId"
                    response:
                      topic: "payment.results"
            """, diagnostics);

        assertEquals(1, steps.size());
        assertEquals(StreamingShape.STREAMING_STREAMING, steps.getFirst().streamingShapeHint());
        java.util.Map<?, ?> dispatch = (java.util.Map<?, ?>) steps.getFirst().awaitConfig().get("dispatch");
        assertEquals("per-item", dispatch.get("mode"));
        assertTrue(diagnostics.stream().noneMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
    }

    @Test
    void rejectsManyToManyAwaitStepWithoutPerItemDispatch() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Bad Await"
                kind: "await"
                cardinality: "MANY_TO_MANY"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "kafka"
                    request:
                      topic: "payment.requests"
                    response:
                      topic: "payment.results"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("await.dispatch.mode=per-item")),
            diagnostics.toString());
    }

    @Test
    void rejectsPerItemDispatchOnUnaryAwaitStep() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Bad Await"
                kind: "await"
                cardinality: "ONE_TO_ONE"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  dispatch:
                    mode: "per-item"
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("only supported for MANY_TO_MANY")),
            diagnostics.toString());
    }

    @Test
    void rejectsUnknownAwaitDispatchMode() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Bad Await"
                kind: "await"
                cardinality: "ONE_TO_ONE"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  dispatch:
                    mode: "peritem"
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("await.dispatch.mode must be one of")),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithServiceField() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Bad Await"
                kind: "await"
                service: "com.example.SomeService"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithoutTimeout() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "No Timeout Await"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithoutAwaitMap() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "No Await Map"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithoutTransportTypeInAwaitMap() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Missing Transport Type"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    url: "https://example.com"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
    }

    @Test
    void rejectsWebhookAwaitStepWithoutRequestUrl() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Webhook Missing Url"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "webhook"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("webhook await transport must declare a URL")),
            diagnostics.toString());
    }

    @Test
    void parsesKafkaAwaitStepDefinition() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Kafka Fraud Check"
                kind: "await"
                input: "com.example.FraudCheckRequest"
                output: "com.example.FraudCheckDecision"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "signedResumeToken"
                  transport:
                    type: "kafka"
                    request:
                      topic: "fraud-check.requests"
                      key: "correlationId"
                    response:
                      topic: "fraud-check.decisions"
            """, diagnostics);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.AWAIT, step.kind());
        java.util.Map<?, ?> transport = (java.util.Map<?, ?>) step.awaitConfig().get("transport");
        assertEquals("kafka", transport.get("type"));
        assertEquals("fraud-check.requests", ((java.util.Map<?, ?>) transport.get("request")).get("topic"));
        assertEquals("fraud-check.decisions", ((java.util.Map<?, ?>) transport.get("response")).get("topic"));
        assertTrue(diagnostics.stream().noneMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())));
    }

    @Test
    void rejectsKafkaAwaitStepWithoutRequestTopic() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Kafka Missing Request Topic"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "kafka"
                    request:
                      key: "interactionId"
                    response:
                      topic: "decisions"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("request.topic")),
            diagnostics.toString());
    }

    @Test
    void rejectsKafkaAwaitStepWithoutResponseTopic() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Kafka Missing Response Topic"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "kafka"
                    request:
                      topic: "requests"
                    response: {}
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("response.topic")),
            diagnostics.toString());
    }

    @Test
    void rejectsKafkaAwaitStepWithInvalidKeyStrategy() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Kafka Invalid Key"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "kafka"
                    request:
                      topic: "requests"
                      key: "orderId"
                    response:
                      topic: "responses"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("request.key")),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithoutCorrelationStrategy() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Missing Correlation"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("await.correlation.strategy must be declared")),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithUnsupportedCorrelationStrategy() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Unsupported Correlation"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "custom"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("unsupported await.correlation.strategy")),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithNonMapCorrelation() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Bad Correlation"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation: "signedResumeToken"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("await.correlation.strategy must be declared")),
            diagnostics.toString());
    }

    @Test
    void rejectsAwaitStepWithoutInputType() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "No Input Await"
                kind: "await"
                output: "com.example.Output"
                timeout: "PT10M"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "webhook"
                    request:
                      url: "https://partner.example/check"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
    }

    @Test
    void rejectsUnsupportedKindValue() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Unknown Kind"
                kind: "unknown"
                service: "com.example.Service"
                input: "com.example.Input"
                output: "com.example.Output"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())),
            diagnostics.toString());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("unsupported kind")),
            diagnostics.toString());
    }

    @Test
    void acceptsAwaitStepWithMultipleIdempotencyKeyFields() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Multi Key Await"
                kind: "await"
                input: "com.example.MultiKeyRequest"
                output: "com.example.MultiKeyResult"
                timeout: "PT30M"
                idempotencyKeyFields: ["orderId", "customerId", "amount"]
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.AWAIT, step.kind());
        assertEquals(List.of("orderId", "customerId", "amount"), step.idempotencyKeyFields());
        assertTrue(diagnostics.stream().noneMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())));
    }

    @Test
    void acceptsAwaitStepWithEmptyIdempotencyKeyFields() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "Empty Keys Await"
                kind: "await"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT5M"
                idempotencyKeyFields: []
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(List.of(), step.idempotencyKeyFields());
        assertTrue(diagnostics.stream().noneMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())));
    }

    @Test
    void awaitKindIsCaseInsensitive() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "AWAIT Step"
                kind: "AWAIT"
                input: "com.example.Input"
                output: "com.example.Output"
                timeout: "PT5M"
                await:
                  correlation:
                    strategy: "interactionId"
                  transport:
                    type: "interaction-api"
            """, diagnostics);

        assertEquals(1, steps.size());
        assertEquals(StepKind.AWAIT, steps.getFirst().kind());
        assertTrue(diagnostics.stream().noneMatch(message -> message.contains(Diagnostic.Kind.ERROR.name())));
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
    void rejectsInternalMapperFieldsForDelegatedStep() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        List<StepDefinition> steps = parse("""
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "bad-delegated"
                operator: "com.example.lib.ExternalService"
                input: "com.example.app.InputType"
                output: "com.example.app.OutputType"
                inboundMapper: "com.example.app.InputMapper"
            """, diagnostics);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()), errorSummary);
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

    @Test
    void parsesRemoteV2StepExecution() throws IOException {
        List<StepDefinition> steps = parse("""
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "charge-card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.contract.ChargeRequest"
                outputTypeName: "com.example.contract.ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 3000
                  target:
                    urlConfigKey: "tpf.remote-operators.charge-card.url"
            """);

        assertEquals(1, steps.size());
        StepDefinition step = steps.getFirst();
        assertEquals(StepKind.REMOTE, step.kind());
        assertNull(step.executionClass());
        assertNotNull(step.remoteExecution());
        assertEquals("charge-card", step.remoteExecution().operatorId());
        assertEquals("PROTOBUF_HTTP_V1", step.remoteExecution().protocol());
        assertEquals(3000, step.remoteExecution().timeoutMs());
        assertEquals("tpf.remote-operators.charge-card.url", step.remoteExecution().target().urlConfigKey());
    }

    @Test
    void rejectsRemoteExecutionOnLegacyVersion() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "charge-card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.contract.ChargeRequest"
                outputTypeName: "com.example.contract.ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  target:
                    url: "https://example.com/operators/charge-card"
            """);
        List<StepDefinition> steps = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message)).parseStepDefinitions(file);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("execution blocks require version: 2"));
    }

    @Test
    void rejectsRemoteExecutionWhenMixedWithLocalServiceFields() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "charge-card"
                service: "com.example.InternalChargeService"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.contract.ChargeRequest"
                outputTypeName: "com.example.contract.ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  target:
                    url: "https://example.com/operators/charge-card"
            """);
        List<StepDefinition> steps = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message)).parseStepDefinitions(file);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("mutually exclusive"));
    }

    @Test
    void rejectsRemoteExecutionWhenCardinalityIsNotUnary() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "charge-card"
                cardinality: "ONE_TO_MANY"
                inputTypeName: "com.example.contract.ChargeRequest"
                outputTypeName: "com.example.contract.ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  target:
                    url: "https://example.com/operators/charge-card"
            """);
        List<StepDefinition> steps = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message)).parseStepDefinitions(file);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("currently supports only ONE_TO_ONE"));
    }

    @Test
    void rejectsInvalidTemplateVersionValue() throws Exception {
        Path file = tempDir.resolve("pipeline.yaml");
        List<String> diagnostics = new ArrayList<>();
        Files.writeString(file, """
            version: "two"
            appName: "Test"
            basePackage: "com.example"
            steps: []
            """);

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new StepDefinitionParser((kind, message) ->
                diagnostics.add(kind + ":" + message)).parseStepDefinitions(file));
        assertTrue(ex.getMessage().contains("Invalid template version"));
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("Invalid template version"));
    }

    @Test
    void rejectsOverflowingRemoteTimeoutValue() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "charge-card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.contract.ChargeRequest"
                outputTypeName: "com.example.contract.ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 3000000000
                  target:
                    url: "https://example.com/operators/charge-card"
            """);

        List<StepDefinition> steps = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message)).parseStepDefinitions(file);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("invalid integer value for execution.timeoutMs"));
    }

    @Test
    void rejectsExecutionBlockWhenModeIsNotRemote() throws IOException {
        List<String> diagnostics = new ArrayList<>();
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, """
            version: 2
            appName: "Test"
            basePackage: "com.example"
            steps:
              - name: "charge-card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.contract.ChargeRequest"
                outputTypeName: "com.example.contract.ChargeResult"
                execution:
                  mode: "LOCAL"
            """);
        List<StepDefinition> steps = new StepDefinitionParser((kind, message) ->
            diagnostics.add(kind + ":" + message)).parseStepDefinitions(file);

        assertTrue(steps.isEmpty());
        String errorSummary = diagnostics.stream().collect(Collectors.joining(" | "));
        assertTrue(errorSummary.contains(Diagnostic.Kind.ERROR.name()));
        assertTrue(errorSummary.contains("expected REMOTE"));
    }

    private List<StepDefinition> parse(String yaml) throws IOException {
        return parse(yaml, null);
    }

    private List<StepDefinition> parse(String yaml, List<String> diagnostics) throws IOException {
        Path file = tempDir.resolve("pipeline.yaml");
        Files.writeString(file, yaml);
        StepDefinitionParser parser = diagnostics == null
            ? new StepDefinitionParser()
            : new StepDefinitionParser((kind, message) -> diagnostics.add(kind + ":" + message));
        return parser.parseStepDefinitions(file);
    }
}
