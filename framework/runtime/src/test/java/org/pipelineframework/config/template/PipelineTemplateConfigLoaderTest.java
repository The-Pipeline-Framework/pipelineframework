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

package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTemplateConfigLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsTemplateConfigWithDefaults() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            aspects:
              persistence:
                enabled: true
                scope: "GLOBAL"
                position: "AFTER_STEP"
                order: 5
                config:
                  enabledTargets:
                    - "GRPC_SERVICE"
              cache:
                enabled: true
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                inputFields:
                  - name: "id"
                    type: "UUID"
                    protoType: "string"
                outputTypeName: "FooOutput"
                outputFields:
                  - name: "status"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();
        PipelineTemplateConfig config = loader.load(configPath);

        assertEquals("Test App", config.appName());
        assertEquals("com.example.test", config.basePackage());
        assertEquals("GRPC", config.transport());
        assertEquals(PipelinePlatform.COMPUTE, config.platform());
        assertEquals(1, config.steps().size());

        PipelineTemplateStep step = config.steps().get(0);
        assertEquals("Process Foo", step.name());
        assertEquals("ONE_TO_ONE", step.cardinality());
        assertEquals("FooInput", step.inputTypeName());
        assertEquals("FooOutput", step.outputTypeName());
        assertEquals(1, step.inputFields().size());
        assertEquals(1, step.outputFields().size());

        Map<String, PipelineTemplateAspect> aspects = config.aspects();
        assertNotNull(aspects);
        assertTrue(aspects.containsKey("persistence"));
        PipelineTemplateAspect persistence = aspects.get("persistence");
        assertTrue(persistence.enabled());
        assertEquals("GLOBAL", persistence.scope());
        assertEquals("AFTER_STEP", persistence.position());
        assertEquals(5, persistence.order());
        Object enabledTargets = persistence.config().get("enabledTargets");
        assertTrue(enabledTargets instanceof List<?>);
        assertTrue(((List<?>) enabledTargets).contains("GRPC_SERVICE"));

        PipelineTemplateAspect cache = aspects.get("cache");
        assertNotNull(cache);
        assertTrue(cache.enabled());
        assertEquals("GLOBAL", cache.scope());
        assertEquals("AFTER_STEP", cache.position());
        assertEquals(0, cache.order());
    }

    @Test
    void platformCanBeOverriddenViaSystemProperty() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "REST"
            platform: "COMPUTE"
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(configPath, yaml);

        Function<String, String> propertyLookup = key -> "pipeline.platform".equals(key) ? "LAMBDA" : null;
        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader(propertyLookup, key -> null);
        PipelineTemplateConfig config = loader.load(configPath);
        assertEquals(PipelinePlatform.FUNCTION, config.platform());
        assertEquals("REST", config.transport());
    }

    @Test
    void loadsExplicitComputePlatformWithoutOverrides() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "REST"
            platform: "COMPUTE"
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config-explicit-compute.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader(key -> null, key -> null);
        PipelineTemplateConfig config = loader.load(configPath);

        assertEquals(PipelinePlatform.COMPUTE, config.platform());
        assertEquals("REST", config.transport());
    }

    @Test
    void loadsV2MessagesAndResolvesStepContracts() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "GRPC"
            messages:
              Money:
                fields:
                  - number: 1
                    name: "amount"
                    type: "decimal"
                  - number: 2
                    name: "currency"
                    type: "currency"
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
                  - number: 2
                    name: "money"
                    type: "Money"
                reserved:
                  numbers: [4, 5]
                  names: ["discount"]
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
                    optional: true
                  - number: 2
                    name: "auditTrail"
                    type: "string"
                    repeated: true
                    deprecated: true
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);

        assertEquals(2, config.version());
        assertTrue(config.messages().containsKey("Money"));
        assertTrue(config.messages().containsKey("ChargeRequest"));
        assertEquals(1, config.steps().size());
        PipelineTemplateStep step = config.steps().getFirst();
        assertEquals("ChargeRequest", step.inputTypeName());
        assertEquals("ChargeResult", step.outputTypeName());
        assertEquals(2, step.inputFields().size());
        assertEquals(2, step.outputFields().size());

        PipelineTemplateField moneyField = step.inputFields().get(1);
        assertEquals("message", moneyField.canonicalType());
        assertEquals("Money", moneyField.messageRef());
        assertEquals("Money", moneyField.protoType());

        PipelineTemplateField resultField = step.outputFields().getFirst();
        assertTrue(resultField.optional());
        assertEquals("uuid", resultField.canonicalType());
        assertEquals("string", resultField.protoType());

        PipelineTemplateField repeatedField = step.outputFields().get(1);
        assertTrue(repeatedField.repeated());
        assertTrue(repeatedField.deprecated());
        assertEquals("List<String>", repeatedField.javaType());

        PipelineTemplateMessage requestMessage = config.messages().get("ChargeRequest");
        assertEquals(List.of(4, 5), requestMessage.reserved().numbers());
        assertEquals(List.of("discount"), requestMessage.reserved().names());
    }

    @Test
    void loadsRemoteExecutionMetadataForV2Steps() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "REST"
            messages:
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 3000
                  target:
                    urlConfigKey: "tpf.remote-operators.charge-card.url"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-remote.yaml");
        Files.writeString(configPath, yaml);

        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateStep step = config.steps().getFirst();

        assertNotNull(step.execution());
        assertTrue(step.execution().isRemote());
        assertEquals("charge-card", step.execution().operatorId());
        assertEquals("PROTOBUF_HTTP_V1", step.execution().protocol());
        assertEquals(3000, step.execution().timeoutMs());
        assertEquals("tpf.remote-operators.charge-card.url", step.execution().target().urlConfigKey());
    }

    @Test
    void rejectsRemoteExecutionWhenCardinalityIsNotUnary() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "REST"
            messages:
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_MANY"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  target:
                    url: "https://example.com/operators/charge-card"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-remote-invalid.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(ex.getMessage().contains("ONE_TO_ONE"));
    }

    @Test
    void rejectsRemoteExecutionWithoutExactlyOneTargetSource() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "REST"
            messages:
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  target:
                    url: "https://example.com/operators/charge-card"
                    urlConfigKey: "tpf.remote-operators.charge-card.url"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-remote-invalid-target.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(ex.getMessage().contains("exactly one"));
    }

    @Test
    void rejectsNonNumericTemplateVersion() throws Exception {
        String yaml = """
            version: "2"
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "REST"
            steps: []
            """;
        Path configPath = tempDir.resolve("pipeline-config-invalid-version.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(ex.getMessage().contains("Template version must be numeric"));
    }

    @Test
    void rejectsLocalExecutionBlockOnV2Step() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "REST"
            messages:
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
                execution:
                  mode: "LOCAL"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-local-execution.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(ex.getMessage().contains("REMOTE mode"));
    }

    @Test
    void rejectsDuplicateFieldNumbersAndNamesWithinMessage() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "GRPC"
            messages:
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
                  - number: 1
                    name: "paymentRef"
                    type: "uuid"
                  - number: 2
                    name: "paymentId"
                    type: "string"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeResult"
                outputTypeName: "ChargeResult"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-duplicate-fields.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(ex.getMessage().contains("Duplicate field number")
            || ex.getMessage().contains("Duplicate field name"));
    }

    @Test
    void rejectsInvalidMapValueTypeInV2Message() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "GRPC"
            messages:
              ChargeResult:
                fields:
                  - number: 1
                    name: "metadata"
                    type: "map"
                    keyType: "string"
                    valueType: "flot32"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeResult"
                outputTypeName: "ChargeResult"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-invalid-map-value.yaml");
        Files.writeString(configPath, yaml);

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineTemplateConfigLoader().load(configPath));
        assertTrue(ex.getMessage().contains("unsupported valueType"));
    }
}
