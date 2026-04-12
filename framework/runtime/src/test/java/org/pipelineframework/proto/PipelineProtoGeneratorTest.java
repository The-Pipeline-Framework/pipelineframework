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
            version: 2
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
            messages:
              FooInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              FooOutput:
                fields:
                  - number: 2
                    name: "status"
                    type: "string"
              BarOutput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
            steps:
              - name: "Process Foo"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FooInput"
                outputTypeName: "FooOutput"
              - name: "Process Bar"
                cardinality: "EXPANSION"
                inputTypeName: "FooOutput"
                outputTypeName: "BarOutput"
            """;
        Path configPath = tempDir.resolve("pipeline-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path fooProtoPath = outputDir.resolve("process-foo-svc.proto");
        Path barProtoPath = outputDir.resolve("process-bar-svc.proto");
        Path orchestratorProtoPath = outputDir.resolve("orchestrator.proto");
        Path typesProtoPath = outputDir.resolve("pipeline-types.proto");

        assertTrue(Files.exists(fooProtoPath));
        assertTrue(Files.exists(barProtoPath));
        assertTrue(Files.exists(orchestratorProtoPath));
        assertTrue(Files.exists(typesProtoPath));

        String fooProto = Files.readString(fooProtoPath);
        String typesProto = Files.readString(typesProtoPath);
        assertTrue(fooProto.contains("package com.example.test;"));
        assertTrue(fooProto.contains("option java_package = \"com.example.test.grpc\";"));
        assertTrue(fooProto.contains("import \"pipeline-types.proto\";"));
        assertTrue(fooProto.contains("service ProcessFooService"));
        assertTrue(fooProto.contains("rpc remoteProcess(FooInput) returns (FooOutput);"));
        assertTrue(fooProto.contains("service ObservePersistenceFooOutputSideEffectService"));
        assertTrue(typesProto.contains("message FooInput"));
        assertTrue(typesProto.contains("string id = 1;"));
        assertTrue(typesProto.contains("message FooOutput"));
        assertTrue(typesProto.contains("string status = 2;"));

        String barProto = Files.readString(barProtoPath);
        assertTrue(barProto.contains("import \"pipeline-types.proto\";"));
        assertTrue(barProto.contains("service ProcessBarService"));
        assertTrue(barProto.contains("rpc remoteProcess(FooOutput) returns (stream BarOutput);"));
        assertTrue(barProto.contains("service ObservePersistenceBarOutputSideEffectService"));
        assertTrue(typesProto.contains("message BarOutput"));

        String orchestratorProto = Files.readString(orchestratorProtoPath);
        assertTrue(orchestratorProto.contains("package com.example.test;"));
        assertTrue(orchestratorProto.contains("import \"pipeline-types.proto\";"));
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
            version: 2
            appName: "Empty Fields Test"
            basePackage: "com.example.empty"
            transport: "GRPC"
            messages:
              ItemInput:
                fields: []
              ItemOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Process Item"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ItemInput"
                outputTypeName: "ItemOutput"
            """;
        Path configPath = tempDir.resolve("empty-fields-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-empty-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path itemProtoPath = outputDir.resolve("process-item-svc.proto");
        assertTrue(Files.exists(itemProtoPath));
        Path typesProtoPath = outputDir.resolve("pipeline-types.proto");
        assertTrue(Files.exists(typesProtoPath));

        String itemProto = Files.readString(itemProtoPath);
        String typesProto = Files.readString(typesProtoPath);
        assertTrue(itemProto.contains("import \"pipeline-types.proto\";"));
        assertTrue(typesProto.contains("message ItemInput"));
        assertTrue(typesProto.contains("message ItemOutput"));
        assertTrue(typesProto.contains("string result = 1;"));
    }

    @Test
    void handlesRestTransportWithoutGeneratingOrchestratorProto() throws Exception {
        String yaml = """
            version: 2
            appName: "REST Test"
            basePackage: "com.example.rest"
            transport: "REST"
            messages:
              DataInput:
                fields:
                  - number: 1
                    name: "data"
                    type: "string"
              DataOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Process Data"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "DataInput"
                outputTypeName: "DataOutput"
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
            version: 2
            appName: "List Fields Test"
            basePackage: "com.example.list"
            transport: "GRPC"
            messages:
              TagsInput:
                fields:
                  - number: 1
                    name: "tags"
                    type: "string"
                    repeated: true
              TagsOutput:
                fields:
                  - number: 2
                    name: "processedTags"
                    type: "string"
                    repeated: true
            steps:
              - name: "Process Tags"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "TagsInput"
                outputTypeName: "TagsOutput"
            """;
        Path configPath = tempDir.resolve("list-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-list-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path tagsProtoPath = outputDir.resolve("process-tags-svc.proto");
        assertTrue(Files.exists(tagsProtoPath));
        Path typesProtoPath = outputDir.resolve("pipeline-types.proto");
        assertTrue(Files.exists(typesProtoPath));

        String typesProto = Files.readString(typesProtoPath);
        assertTrue(typesProto.contains("repeated string tags = 1;"));
        assertTrue(typesProto.contains("repeated string processedTags = 2;"));
    }

    @Test
    void generatesProtoForMapFields() throws Exception {
        String yaml = """
            version: 2
            appName: "Map Fields Test"
            basePackage: "com.example.map"
            transport: "GRPC"
            messages:
              MetadataInput:
                fields:
                  - number: 1
                    name: "metadata"
                    type: "map"
                    keyType: "string"
                    valueType: "string"
              MetadataOutput:
                fields:
                  - number: 1
                    name: "processed"
                    type: "string"
            steps:
              - name: "Process Metadata"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "MetadataInput"
                outputTypeName: "MetadataOutput"
            """;
        Path configPath = tempDir.resolve("map-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-map-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path metadataProtoPath = outputDir.resolve("process-metadata-svc.proto");
        assertTrue(Files.exists(metadataProtoPath));
        Path typesProtoPath = outputDir.resolve("pipeline-types.proto");
        assertTrue(Files.exists(typesProtoPath));

        String typesProto = Files.readString(typesProtoPath);
        assertTrue(typesProto.contains("map<string, string> metadata = 1;"));
    }

    @Test
    void generatesAspectServicesForBeforeStepAspects() throws Exception {
        String yaml = """
            version: 2
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
            messages:
              ItemInput:
                fields:
                  - number: 1
                    name: "value"
                    type: "string"
              ItemOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Process Item"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ItemInput"
                outputTypeName: "ItemOutput"
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
            version: 2
            appName: "Many to Many Test"
            basePackage: "com.example.m2m"
            transport: "GRPC"
            messages:
              BatchInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "string"
              BatchOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Process Batch"
                cardinality: "MANY_TO_MANY"
                inputTypeName: "BatchInput"
                outputTypeName: "BatchOutput"
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

        assertThrows(IllegalArgumentException.class, () ->
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
            version: 2
            appName: "Alias Test"
            basePackage: "com.example.alias"
            transport: "GRPC"
            messages:
              DocInput:
                fields:
                  - number: 1
                    name: "docId"
                    type: "uuid"
              TokenBatch:
                fields:
                  - number: 2
                    name: "tokens"
                    type: "string"
              IndexAck:
                fields:
                  - number: 2
                    name: "status"
                    type: "string"
            steps:
              - name: "Tokenize"
                cardinality: "ONE_TO_MANY"
                inputTypeName: "DocInput"
                outputTypeName: "TokenBatch"
              - name: "Index"
                cardinality: "MANY_TO_ONE"
                inputTypeName: "TokenBatch"
                outputTypeName: "IndexAck"
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
        Path typesProtoPath = outputDir.resolve("pipeline-types.proto");
        assertTrue(Files.exists(typesProtoPath));

        String tokenizeProto = Files.readString(tokenizeProtoPath);
        String indexProto = Files.readString(indexProtoPath);
        String orchestratorProto = Files.readString(orchestratorProtoPath);
        String typesProto = Files.readString(typesProtoPath);

        assertTrue(tokenizeProto.contains("package com.example.alias;"));
        assertTrue(tokenizeProto.contains("import \"pipeline-types.proto\";"));
        assertTrue(tokenizeProto.contains("rpc remoteProcess(DocInput) returns (stream TokenBatch);"));
        assertTrue(typesProto.contains("message DocInput"));
        assertTrue(typesProto.contains("string docId = 1;"));
        assertTrue(typesProto.contains("message TokenBatch"));
        assertTrue(typesProto.contains("string tokens = 2;"));

        assertTrue(indexProto.contains("package com.example.alias;"));
        assertTrue(indexProto.contains("import \"pipeline-types.proto\";"));
        assertTrue(indexProto.contains("rpc remoteProcess(stream TokenBatch) returns (IndexAck);"));
        assertTrue(typesProto.contains("message IndexAck"));
        assertTrue(typesProto.contains("string status = 2;"));

        assertTrue(orchestratorProto.contains("package com.example.alias;"));
        assertTrue(orchestratorProto.contains("import \"pipeline-types.proto\";"));
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
    void failsCompatibilityCheckWhenFieldNameChangesAtExistingNumber() throws Exception {
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

    @Test
    void rendersMultilineFieldCommentsAsSeparateProtoComments() throws Exception {
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
                    comment: |
                      first line
                      second line
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
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-comments.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-v2-comments-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        String typesContent = Files.readString(outputDir.resolve("pipeline-types.proto"));
        assertTrue(typesContent.contains("  // first line"));
        assertTrue(typesContent.contains("  // second line"));
    }

    @Test
    @ClearSystemProperty(key = "tpf.idl.compat.baseline")
    void rejectsInvalidCompatibilityBaselinePath() throws Exception {
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
                    name: "paymentId"
                    type: "uuid"
            steps:
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ChargeRequest"
                outputTypeName: "ChargeResult"
            """;
        Path configPath = tempDir.resolve("pipeline-config-v2-invalid-baseline.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-v2-invalid-baseline-out");

        System.setProperty("tpf.idl.compat.baseline", "\u0000bad-path");
        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new PipelineProtoGenerator().generate(tempDir, configPath, outputDir));
        assertTrue(ex.getMessage().contains("Invalid IDL compatibility baseline path"));
    }
}
