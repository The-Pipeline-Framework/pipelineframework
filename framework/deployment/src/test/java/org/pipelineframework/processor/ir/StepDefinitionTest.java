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

package org.pipelineframework.processor.ir;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateRemoteTarget;
import org.pipelineframework.config.template.PipelineTemplateStepExecution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StepDefinitionTest {

    private static final ClassName INPUT_TYPE = ClassName.get("com.example", "Input");
    private static final ClassName OUTPUT_TYPE = ClassName.get("com.example", "Output");
    private static final ClassName EXECUTION_CLASS = ClassName.get("com.example", "ServiceImpl");

    // ---- AWAIT kind tests ----

    @Test
    void constructsAwaitStepWithRequiredFields() {
        StepDefinition step = new StepDefinition(
            "Fraud Check",
            StepKind.AWAIT,
            null,        // executionClass
            null,        // remoteExecution
            Map.of("transport", Map.of("type", "webhook")),
            "PT10M",
            List.of("orderId"),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE,
            OUTPUT_TYPE,
            StreamingShape.UNARY_UNARY);

        assertEquals("Fraud Check", step.name());
        assertEquals(StepKind.AWAIT, step.kind());
        assertNull(step.executionClass());
        assertNull(step.remoteExecution());
        assertEquals("PT10M", step.timeout());
        assertEquals(List.of("orderId"), step.idempotencyKeyFields());
        assertEquals(INPUT_TYPE, step.inputType());
        assertEquals(OUTPUT_TYPE, step.outputType());
    }

    @Test
    void awaitStepDefaultsAwaitConfigToEmptyMapWhenNull() {
        StepDefinition step = new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            null, null,
            null,  // awaitConfig null -> should be normalized to Map.of()
            "PT5M",
            null,  // idempotencyKeyFields null -> should be normalized to List.of()
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE,
            OUTPUT_TYPE,
            null);

        assertEquals(Map.of(), step.awaitConfig());
        assertEquals(List.of(), step.idempotencyKeyFields());
    }

    @Test
    void awaitStepMakesImmutableCopyOfAwaitConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("key", "value");

        StepDefinition step = new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            null, null,
            config,
            "PT5M",
            List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        config.put("extra", "extra-value");  // mutate original
        assertEquals(1, step.awaitConfig().size());
        assertThrows(UnsupportedOperationException.class, () -> step.awaitConfig().put("k", "v"));
    }

    @Test
    void awaitStepMakesImmutableCopyOfIdempotencyKeyFields() {
        List<String> fields = new ArrayList<>();
        fields.add("orderId");

        StepDefinition step = new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            null, null,
            Map.of(),
            "PT5M",
            fields,
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        fields.add("customerId");  // mutate original
        assertEquals(1, step.idempotencyKeyFields().size());
        assertThrows(UnsupportedOperationException.class, () -> step.idempotencyKeyFields().add("x"));
    }

    @Test
    void awaitStepRejectsNullInputType() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            null, null,
            Map.of(), "PT5M", List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            null,  // null inputType
            OUTPUT_TYPE, null));
    }

    @Test
    void awaitStepRejectsNullOutputType() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            null, null,
            Map.of(), "PT5M", List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE,
            null,  // null outputType
            null));
    }

    @Test
    void awaitStepRejectsExecutionClass() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            EXECUTION_CLASS,  // should not be set for AWAIT
            null,
            Map.of(), "PT5M", List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void awaitStepRejectsRemoteExecution() {
        PipelineTemplateStepExecution remoteExecution = new PipelineTemplateStepExecution(
            "REMOTE", "operator-id", "PROTOBUF_HTTP_V1", 3000,
            PipelineTemplateRemoteTarget.ofUrlConfigKey("some.config.key"));
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "Await Step",
            StepKind.AWAIT,
            null,
            remoteExecution,  // should not be set for AWAIT
            Map.of(), "PT5M", List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    // ---- requireNonRemoteKind tests ----

    @Test
    void convenienceConstructorRejectsAwaitKind() {
        // The three-param convenience constructors use requireNonRemoteKind which now also rejects AWAIT
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "step",
            StepKind.AWAIT,
            EXECUTION_CLASS,
            null,  // externalMapper
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void convenienceConstructorRejectsRemoteKind() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "step",
            StepKind.REMOTE,
            EXECUTION_CLASS,
            null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    // ---- non-AWAIT validation ----

    @Test
    void internalStepRequiresExecutionClass() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "internal-step",
            StepKind.INTERNAL,
            null,  // no execution class
            null,
            Map.of(), null, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void rejectsBlankName() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "  ",  // blank name
            StepKind.INTERNAL,
            EXECUTION_CLASS,
            null,
            Map.of(), null, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void rejectsNullName() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            null,
            StepKind.INTERNAL,
            EXECUTION_CLASS,
            null,
            Map.of(), null, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void rejectsNullKind() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "step",
            null,  // null kind
            EXECUTION_CLASS,
            null,
            Map.of(), null, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void rejectsMutuallyExclusiveExecutionClassAndRemoteExecution() {
        PipelineTemplateStepExecution remoteExecution = new PipelineTemplateStepExecution(
            "REMOTE", "op-id", "PROTOBUF_HTTP_V1", 3000,
            PipelineTemplateRemoteTarget.ofUrlConfigKey("some.config.key"));
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "step",
            StepKind.REMOTE,
            EXECUTION_CLASS,  // both set
            remoteExecution,
            Map.of(), null, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void mapperFallbackDefaultsToNoneWhenNull() {
        StepDefinition step = new StepDefinition(
            "step",
            StepKind.INTERNAL,
            EXECUTION_CLASS,
            null,
            Map.of(), null, List.of(),
            null, null, null,
            null,  // null -> should default to NONE
            INPUT_TYPE, OUTPUT_TYPE, null);

        assertEquals(MapperFallbackMode.NONE, step.mapperFallback());
    }

    @Test
    void internalStepHasEmptyAwaitConfigByDefault() {
        StepDefinition step = new StepDefinition(
            "step",
            StepKind.INTERNAL,
            EXECUTION_CLASS,
            null,
            null, null, null,  // all await fields null
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        assertEquals(Map.of(), step.awaitConfig());
        assertEquals(List.of(), step.idempotencyKeyFields());
        assertNull(step.timeout());
    }

    // ---- QUERY kind tests ----

    @Test
    void constructsQueryStepWithRequiredFields() {
        StepDefinition step = new StepDefinition(
            "Load Customer Risk",
            StepKind.QUERY,
            null,
            null,
            Map.of(),
            null,
            List.of(),
            "customer-risk-by-id",
            Map.of("source", "risk-db"),
            List.of("customerId"),
            null,
            null,
            null,
            MapperFallbackMode.NONE,
            INPUT_TYPE,
            OUTPUT_TYPE,
            StreamingShape.UNARY_UNARY);

        assertEquals("Load Customer Risk", step.name());
        assertEquals(StepKind.QUERY, step.kind());
        assertNull(step.executionClass());
        assertNull(step.remoteExecution());
        assertEquals("customer-risk-by-id", step.queryId());
        assertEquals(List.of("customerId"), step.queryKeyFields());
        assertEquals(Map.of("source", "risk-db"), step.queryConfig());
        assertEquals(INPUT_TYPE, step.inputType());
        assertEquals(OUTPUT_TYPE, step.outputType());
    }

    @Test
    void queryStepRejectsNullInputType() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            "query-id", Map.of(), List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            null,  // null inputType
            OUTPUT_TYPE, null));
    }

    @Test
    void queryStepRejectsNullOutputType() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            "query-id", Map.of(), List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE,
            null,  // null outputType
            null));
    }

    @Test
    void queryStepRejectsNullQueryId() {
        assertThrows(NullPointerException.class, () -> new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            null,  // null queryId
            Map.of(), List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void queryStepRejectsBlankQueryId() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            "   ",  // blank queryId
            Map.of(), List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void queryStepRejectsExecutionClass() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            EXECUTION_CLASS,  // must be null
            null,
            Map.of(), null, List.of(),
            "query-id", Map.of(), List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void queryStepRejectsRemoteExecution() {
        org.pipelineframework.config.template.PipelineTemplateStepExecution remoteExecution =
            new org.pipelineframework.config.template.PipelineTemplateStepExecution(
                "REMOTE", "op-id", "PROTOBUF_HTTP_V1", 3000,
                org.pipelineframework.config.template.PipelineTemplateRemoteTarget.ofUrlConfigKey("cfg"));

        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null,
            remoteExecution,  // must be null
            Map.of(), null, List.of(),
            "query-id", Map.of(), List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void queryStepDefaultsQueryConfigToEmptyMapWhenNull() {
        StepDefinition step = new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            "query-id",
            null,  // null queryConfig
            null,  // null queryKeyFields
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        assertEquals(Map.of(), step.queryConfig());
        assertEquals(List.of(), step.queryKeyFields());
    }

    @Test
    void queryStepMakesImmutableCopyOfQueryConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("source", "risk-db");
        StepDefinition step = new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            "query-id", config, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        config.put("extra", "value");

        assertEquals(1, step.queryConfig().size());
        assertThrows(UnsupportedOperationException.class, () -> step.queryConfig().put("k", "v"));
    }

    @Test
    void queryStepMakesImmutableCopyOfQueryKeyFields() {
        List<String> fields = new ArrayList<>();
        fields.add("customerId");
        StepDefinition step = new StepDefinition(
            "Load Risk",
            StepKind.QUERY,
            null, null,
            Map.of(), null, List.of(),
            "query-id", Map.of(), fields,
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        fields.add("tenantId");

        assertEquals(1, step.queryKeyFields().size());
        assertThrows(UnsupportedOperationException.class, () -> step.queryKeyFields().add("x"));
    }

    @Test
    void convenienceConstructorRejectsQueryKind() {
        assertThrows(IllegalArgumentException.class, () -> new StepDefinition(
            "step",
            StepKind.QUERY,
            EXECUTION_CLASS,
            null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null));
    }

    @Test
    void internalStepDefaultsQueryFieldsToEmptyCollections() {
        StepDefinition step = new StepDefinition(
            "step",
            StepKind.INTERNAL,
            EXECUTION_CLASS,
            null,
            Map.of(), null, List.of(),
            null, null, null,
            MapperFallbackMode.NONE,
            INPUT_TYPE, OUTPUT_TYPE, null);

        assertNull(step.queryId());
        assertEquals(Map.of(), step.queryConfig());
        assertEquals(List.of(), step.queryKeyFields());
    }
}