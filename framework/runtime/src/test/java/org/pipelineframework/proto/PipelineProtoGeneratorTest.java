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

package org.pipelineframework.proto;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.ClearSystemProperty;

import static org.junit.jupiter.api.Assertions.*;

class PipelineProtoGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesStepAndOrchestratorProtos() throws Exception {
        String yaml = """
            appName: "Test App"
            basePackage: "com.example.test"
            transport: "GRPC"
            aspects:
              persistence:
                enabled: true
                scope: "GLOBAL"
                position: "AFTER_STEP"
                config:
                  enabledTargets:
                    - "GRPC_SERVICE"
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
              - name: "Process Bar"
                cardinality: "EXPANSION"
                inputTypeName: "FooOutput"
                inputFields:
                  - name: "status"
                    type: "String"
                    protoType: "string"
                outputTypeName: "BarOutput"
                outputFields:
                  - name: "id"
                    type: "UUID"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path fooProtoPath = outputDir.resolve("process-foo-svc.proto");
        Path barProtoPath = outputDir.resolve("process-bar-svc.proto");
        Path orchestratorProtoPath = outputDir.resolve("orchestrator.proto");

        assertTrue(Files.exists(fooProtoPath));
        assertTrue(Files.exists(barProtoPath));
        assertTrue(Files.exists(orchestratorProtoPath));

        String fooProto = Files.readString(fooProtoPath);
        assertTrue(fooProto.contains("package com.example.test;"));
        assertTrue(fooProto.contains("option java_package = \"com.example.test.grpc\";"));
        assertTrue(fooProto.contains("message FooInput"));
        assertTrue(fooProto.contains("string id = 1;"));
        assertTrue(fooProto.contains("message FooOutput"));
        assertTrue(fooProto.contains("string status = 2;"));
        assertTrue(fooProto.contains("service ProcessFooService"));
        assertTrue(fooProto.contains("rpc remoteProcess(FooInput) returns (FooOutput);"));
        assertTrue(fooProto.contains("service ObservePersistenceFooOutputSideEffectService"));

        String barProto = Files.readString(barProtoPath);
        assertTrue(barProto.contains("import \"process-foo-svc.proto\";"));
        assertTrue(barProto.contains("message BarOutput"));
        assertTrue(barProto.contains("service ProcessBarService"));
        assertTrue(barProto.contains("rpc remoteProcess(FooOutput) returns (stream BarOutput);"));
        assertTrue(barProto.contains("service ObservePersistenceBarOutputSideEffectService"));

        String orchestratorProto = Files.readString(orchestratorProtoPath);
        assertTrue(orchestratorProto.contains("package com.example.test;"));
        assertTrue(orchestratorProto.contains("import \"process-foo-svc.proto\";"));
        assertTrue(orchestratorProto.contains("import \"process-bar-svc.proto\";"));
        assertTrue(orchestratorProto.contains("service OrchestratorService"));
        assertTrue(orchestratorProto.contains("rpc Run (FooInput) returns (stream BarOutput);"));
        assertTrue(orchestratorProto.contains("rpc Ingest (stream FooInput) returns (stream BarOutput);"));
        assertTrue(orchestratorProto.contains("message RunAsyncRequest"));
        assertTrue(orchestratorProto.contains("message RunAsyncResponse"));
        assertTrue(orchestratorProto.contains("message GetExecutionStatusRequest"));
        assertTrue(orchestratorProto.contains("message GetExecutionStatusResponse"));
        assertTrue(orchestratorProto.contains("message GetExecutionResultRequest"));
        assertTrue(orchestratorProto.contains("message GetExecutionResultResponse"));
        assertTrue(orchestratorProto.contains("rpc RunAsync (RunAsyncRequest) returns (RunAsyncResponse);"));
        assertTrue(orchestratorProto.contains(
            "rpc GetExecutionStatus (GetExecutionStatusRequest) returns (GetExecutionStatusResponse);"));
        assertTrue(orchestratorProto.contains(
            "rpc GetExecutionResult (GetExecutionResultRequest) returns (GetExecutionResultResponse);"));
        assertTrue(orchestratorProto.contains("rpc Subscribe (google.protobuf.Empty) returns (stream BarOutput);"));
    }

    @Test
    void generatesProtoWithEmptyInputFields() throws Exception {
        String yaml = """
            appName: "Empty Fields Test"
            basePackage: "com.example.empty"
            transport: "GRPC"
            steps:
              - name: "Process Item"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ItemInput"
                inputFields: []
                outputTypeName: "ItemOutput"
                outputFields:
                  - name: "result"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("empty-fields-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-empty-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path itemProtoPath = outputDir.resolve("process-item-svc.proto");
        assertTrue(Files.exists(itemProtoPath));

        String itemProto = Files.readString(itemProtoPath);
        assertTrue(itemProto.contains("message ItemInput"));
        assertTrue(itemProto.contains("message ItemOutput"));
        assertTrue(itemProto.contains("string result = 1;"));
    }

    @Test
    void handlesRestTransportWithoutGeneratingOrchestratorProto() throws Exception {
        String yaml = """
            appName: "REST Test"
            basePackage: "com.example.rest"
            transport: "REST"
            steps:
              - name: "Process Data"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "DataInput"
                inputFields:
                  - name: "data"
                    type: "String"
                    protoType: "string"
                outputTypeName: "DataOutput"
                outputFields:
                  - name: "result"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("rest-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-rest-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path dataProtoPath = outputDir.resolve("process-data-svc.proto");
        Path orchestratorProtoPath = outputDir.resolve("orchestrator.proto");

        assertTrue(Files.exists(dataProtoPath));
        assertFalse(Files.exists(orchestratorProtoPath));
    }

    @Test
    void generatesProtoForListFields() throws Exception {
        String yaml = """
            appName: "List Fields Test"
            basePackage: "com.example.list"
            transport: "GRPC"
            steps:
              - name: "Process Tags"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "TagsInput"
                inputFields:
                  - name: "tags"
                    type: "List<String>"
                    protoType: "string"
                outputTypeName: "TagsOutput"
                outputFields:
                  - name: "processedTags"
                    type: "List<String>"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("list-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-list-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path tagsProtoPath = outputDir.resolve("process-tags-svc.proto");
        assertTrue(Files.exists(tagsProtoPath));

        String tagsProto = Files.readString(tagsProtoPath);
        assertTrue(tagsProto.contains("repeated string tags = 1;"));
        assertTrue(tagsProto.contains("repeated string processedTags = 2;"));
    }

    @Test
    void generatesProtoForMapFields() throws Exception {
        String yaml = """
            appName: "Map Fields Test"
            basePackage: "com.example.map"
            transport: "GRPC"
            steps:
              - name: "Process Metadata"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "MetadataInput"
                inputFields:
                  - name: "metadata"
                    type: "Map<String, String>"
                    protoType: "string"
                outputTypeName: "MetadataOutput"
                outputFields:
                  - name: "processed"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("map-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-map-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path metadataProtoPath = outputDir.resolve("process-metadata-svc.proto");
        assertTrue(Files.exists(metadataProtoPath));

        String metadataProto = Files.readString(metadataProtoPath);
        assertTrue(metadataProto.contains("map<string, string> metadata = 1;"));
    }

    @Test
    void generatesAspectServicesForBeforeStepAspects() throws Exception {
        String yaml = """
            appName: "Before Aspect Test"
            basePackage: "com.example.before"
            transport: "GRPC"
            aspects:
              validation:
                enabled: true
                scope: "GLOBAL"
                position: "BEFORE_STEP"
                config:
                  enabledTargets:
                    - "GRPC_SERVICE"
            steps:
              - name: "Process Item"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ItemInput"
                inputFields:
                  - name: "value"
                    type: "String"
                    protoType: "string"
                outputTypeName: "ItemOutput"
                outputFields:
                  - name: "result"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("before-aspect-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-before-aspect-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path itemProtoPath = outputDir.resolve("process-item-svc.proto");
        assertTrue(Files.exists(itemProtoPath));

        String itemProto = Files.readString(itemProtoPath);
        assertTrue(itemProto.contains("service ObserveValidationItemInputSideEffectService"));
    }

    @Test
    void generatesManyToManyServiceSignature() throws Exception {
        String yaml = """
            appName: "Many to Many Test"
            basePackage: "com.example.m2m"
            transport: "GRPC"
            steps:
              - name: "Process Batch"
                cardinality: "MANY_TO_MANY"
                inputTypeName: "BatchInput"
                inputFields:
                  - name: "id"
                    type: "String"
                    protoType: "string"
                outputTypeName: "BatchOutput"
                outputFields:
                  - name: "result"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("m2m-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-m2m-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path batchProtoPath = outputDir.resolve("process-batch-svc.proto");
        assertTrue(Files.exists(batchProtoPath));

        String batchProto = Files.readString(batchProtoPath);
        assertTrue(batchProto.contains("rpc remoteProcess(stream BatchInput) returns (stream BatchOutput);"));
    }

    @Test
    void throwsExceptionWhenBasePackageMissing() {
        String yaml = """
            appName: "No Package Test"
            transport: "GRPC"
            steps:
              - name: "Process Item"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ItemInput"
                outputTypeName: "ItemOutput"
            """;
        Path configPath = tempDir.resolve("no-package-config.yaml");
        assertDoesNotThrow(() -> Files.writeString(configPath, yaml));
        Path outputDir = tempDir.resolve("proto-no-package-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();

        assertThrows(IllegalStateException.class, () ->
            generator.generate(tempDir, configPath, outputDir));
    }

    @Test
    void handlesEmptyStepsList() throws Exception {
        String yaml = """
            appName: "Empty Steps Test"
            basePackage: "com.example.empty"
            transport: "GRPC"
            steps: []
            """;
        Path configPath = tempDir.resolve("empty-steps-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-empty-steps-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        try (var paths = Files.list(outputDir)) {
            assertFalse(paths.findAny().isPresent());
        }
    }

    @Test
    void supportsOneToManyAndManyToOneRelationships() throws Exception {
        String yaml = """
            appName: "Alias Test"
            basePackage: "com.example.alias"
            transport: "GRPC"
            steps:
              - name: "Tokenize"
                cardinality: "ONE_TO_MANY"
                inputTypeName: "DocInput"
                inputFields:
                  - name: "docId"
                    type: "UUID"
                    protoType: "string"
                outputTypeName: "TokenBatch"
                outputFields:
                  - name: "tokens"
                    type: "String"
                    protoType: "string"
              - name: "Index"
                cardinality: "MANY_TO_ONE"
                inputTypeName: "TokenBatch"
                inputFields:
                  - name: "tokens"
                    type: "String"
                    protoType: "string"
                outputTypeName: "IndexAck"
                outputFields:
                  - name: "status"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("pipeline-alias-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-alias-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path tokenizeProtoPath = outputDir.resolve("tokenize-svc.proto");
        Path indexProtoPath = outputDir.resolve("index-svc.proto");
        Path orchestratorProtoPath = outputDir.resolve("orchestrator.proto");
        assertTrue(Files.exists(tokenizeProtoPath));
        assertTrue(Files.exists(indexProtoPath));
        assertTrue(Files.exists(orchestratorProtoPath));

        String tokenizeProto = Files.readString(tokenizeProtoPath);
        String indexProto = Files.readString(indexProtoPath);
        String orchestratorProto = Files.readString(orchestratorProtoPath);

        assertTrue(tokenizeProto.contains("package com.example.alias;"));
        assertTrue(tokenizeProto.contains("message DocInput"));
        assertTrue(tokenizeProto.contains("string docId = 1;"));
        assertTrue(tokenizeProto.contains("message TokenBatch"));
        assertTrue(tokenizeProto.contains("string tokens = 2;"));
        assertTrue(tokenizeProto.contains("rpc remoteProcess(DocInput) returns (stream TokenBatch);"));

        assertTrue(indexProto.contains("package com.example.alias;"));
        assertTrue(indexProto.contains("import \"tokenize-svc.proto\";"));
        assertTrue(indexProto.contains("message IndexAck"));
        assertTrue(indexProto.contains("string status = 2;"));
        assertTrue(indexProto.contains("rpc remoteProcess(stream TokenBatch) returns (IndexAck);"));

        assertTrue(orchestratorProto.contains("package com.example.alias;"));
        assertTrue(orchestratorProto.contains("import \"tokenize-svc.proto\";"));
        assertTrue(orchestratorProto.contains("import \"index-svc.proto\";"));
        assertTrue(orchestratorProto.contains("service OrchestratorService"));
        assertTrue(orchestratorProto.contains("rpc Run (DocInput) returns (IndexAck);"));
        assertTrue(orchestratorProto.contains("rpc RunAsync (RunAsyncRequest) returns (RunAsyncResponse);"));
        assertTrue(orchestratorProto.contains(
            "rpc GetExecutionStatus (GetExecutionStatusRequest) returns (GetExecutionStatusResponse);"));
        assertTrue(orchestratorProto.contains(
            "rpc GetExecutionResult (GetExecutionResultRequest) returns (GetExecutionResultResponse);"));
    }

    @Test
    void generatesV2TypesProtoAndIdlSnapshot() throws Exception {
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "GRPC"
            messages:
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
                  - number: 2
                    name: "amount"
                    type: "decimal"
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
                reserved:
                  numbers: [5]
                  names: ["legacyCode"]
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-v2-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path typesProto = outputDir.resolve("pipeline-types.proto");
        Path stepProto = outputDir.resolve("charge-card-svc.proto");
        Path orchestratorProto = outputDir.resolve("orchestrator.proto");
        Path idlSnapshot = tempDir.resolve("target/generated-resources/META-INF/pipeline/idl.json");

        assertTrue(Files.exists(typesProto));
        assertTrue(Files.exists(stepProto));
        assertTrue(Files.exists(orchestratorProto));
        assertTrue(Files.exists(idlSnapshot));

        String typesContent = Files.readString(typesProto);
        assertTrue(typesContent.contains("message ChargeRequest"));
        assertTrue(typesContent.contains("string amount = 2;"));
        assertTrue(typesContent.contains("message ChargeResult"));
        assertTrue(typesContent.contains("reserved 5;"));
        assertTrue(typesContent.contains("reserved \"legacyCode\";"));
        assertTrue(typesContent.contains("optional string paymentId = 1;"));
        assertTrue(typesContent.contains("repeated string auditTrail = 2 [deprecated = true];"));

        String stepContent = Files.readString(stepProto);
        assertTrue(stepContent.contains("import \"pipeline-types.proto\";"));
        assertTrue(stepContent.contains("rpc remoteProcess(ChargeRequest) returns (ChargeResult);"));

        String orchestratorContent = Files.readString(orchestratorProto);
        assertTrue(orchestratorContent.contains("import \"pipeline-types.proto\";"));
        assertTrue(orchestratorContent.contains("rpc Run (ChargeRequest) returns (ChargeResult);"));

        String snapshot = Files.readString(idlSnapshot);
        assertTrue(snapshot.contains("\"messages\""));
        assertTrue(snapshot.contains("\"ChargeRequest\""));
        assertTrue(snapshot.contains("\"ChargeResult\""));
    }

    @Test
    @ClearSystemProperty(key = "tpf.idl.compat.baseline")
    void failsCompatibilityCheckWhenFieldNumberIsReusedWithoutReserve() throws Exception {
        String baselineJson = """
            {
              "version": 2,
              "appName": "Baseline",
              "basePackage": "com.example.v2",
              "messages": {
                "ChargeRequest": {
                  "name": "ChargeRequest",
                  "fields": [
                    {
                      "number": 1,
                      "name": "orderId",
                      "canonicalType": "uuid",
                      "messageRef": null,
                      "keyType": null,
                      "valueType": null,
                      "optional": false,
                      "repeated": false,
                      "deprecated": false,
                      "protoType": "string"
                    }
                  ],
                  "reservedNumbers": [],
                  "reservedNames": []
                },
                "ChargeResult": {
                  "name": "ChargeResult",
                  "fields": [
                    {
                      "number": 1,
                      "name": "paymentId",
                      "canonicalType": "uuid",
                      "messageRef": null,
                      "keyType": null,
                      "valueType": null,
                      "optional": false,
                      "repeated": false,
                      "deprecated": false,
                      "protoType": "string"
                    }
                  ],
                  "reservedNumbers": [],
                  "reservedNames": []
                }
              },
              "steps": [
                {
                  "name": "Charge Card",
                  "inputTypeName": "ChargeRequest",
                  "outputTypeName": "ChargeResult"
                }
              ]
            }
            """;
        Path baselinePath = tempDir.resolve("baseline-idl.json");
        Files.writeString(baselinePath, baselineJson);
        String yaml = """
            version: 2
            appName: "IDL v2"
            basePackage: "com.example.v2"
            transport: "GRPC"
            messages:
              ChargeRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              ChargeResult:
                fields:
                  - number: 1
                    name: "status"
                    type: "string"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-breaking.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-v2-compat-out");

        System.setProperty("tpf.idl.compat.baseline", baselinePath.toString());
        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineProtoGenerator().generate(tempDir, configPath, outputDir));
        assertTrue(ex.getMessage().contains("IDL compatibility check failed"));
        assertTrue(ex.getMessage().contains("changed field name at number 1"));
    }
}
