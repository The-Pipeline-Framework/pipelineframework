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
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.pipelineframework.config.pipeline.PipelineJson;

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
        assertTrue(orchestratorProto.contains("message CompleteAwaitRequest"));
        assertTrue(orchestratorProto.contains("string resume_token = 7;"),
            "resume_token tag 7 is a wire-compatibility contract");
        assertTrue(orchestratorProto.contains("message CompleteAwaitResponse {"));
        assertTrue(orchestratorProto.contains("message ListPendingAwaitRequest"));
        assertTrue(orchestratorProto.contains("message ListPendingAwaitResponse {"));
        assertTrue(orchestratorProto.contains("string request_payload_json = 12;"));
        assertTrue(orchestratorProto.contains("rpc RunAsync (RunAsyncRequest) returns (RunAsyncResponse);"));
        assertTrue(orchestratorProto.contains(
            "rpc GetExecutionStatus (GetExecutionStatusRequest) returns (GetExecutionStatusResponse);"));
        assertTrue(orchestratorProto.contains(
            "rpc GetExecutionResult (GetExecutionResultRequest) returns (GetExecutionResultResponse);"));
        assertTrue(orchestratorProto.contains("rpc CompleteAwait (CompleteAwaitRequest) returns (CompleteAwaitResponse);"));
        assertTrue(orchestratorProto.contains("rpc ListPendingAwait (ListPendingAwaitRequest) returns (ListPendingAwaitResponse);"));
        assertTrue(orchestratorProto.contains("rpc Subscribe (google.protobuf.Empty) returns (stream BarOutput);"));
    }

    @Test
    void generatesOneofForUnionOutput() throws Exception {
        String yaml = """
            version: 2
            appName: "Union Test"
            basePackage: "com.example.union"
            transport: "GRPC"
            messages:
              PaymentRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentCaptured:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              PaymentRejected:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
                  - number: 2
                    name: "failureCode"
                    type: "string"
            unions:
              PaymentOutcome:
                variants:
                  captured:
                    type: PaymentCaptured
                    number: 1
                  rejected:
                    type: PaymentRejected
                    number: 2
            steps:
              - name: "Capture Payment"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRequest"
                outputTypeName: "PaymentOutcome"
            """;
        Path configPath = tempDir.resolve("union-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-union-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        String stepProto = Files.readString(outputDir.resolve("capture-payment-svc.proto"));
        String typesProto = Files.readString(outputDir.resolve("pipeline-types.proto"));
        assertTrue(stepProto.contains("rpc remoteProcess(PaymentRequest) returns (PaymentOutcome);"));
        assertTrue(typesProto.contains("message PaymentOutcome"));
        assertTrue(typesProto.contains("oneof outcome"));
        assertTrue(typesProto.contains("PaymentCaptured captured = 1;"));
        assertTrue(typesProto.contains("PaymentRejected rejected = 2;"));
    }

    @Test
    void preservesDeclaredBranchStepInputTypesForV2Contracts() throws Exception {
        String yaml = """
            version: 2
            appName: "Branch Routing Test"
            basePackage: "com.example.branch"
            transport: "GRPC"
            messages:
              PaymentRecord:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              ApprovedPaymentStatus:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              UnapprovedPaymentStatus:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
                  - number: 2
                    name: "message"
                    type: "string"
              ApprovedPaymentOutput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              UnapprovedPaymentOutput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
                  - number: 2
                    name: "message"
                    type: "string"
              PaymentOutput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
                  - number: 2
                    name: "message"
                    type: "string"
            unions:
              PaymentStatus:
                variants:
                  approved:
                    type: ApprovedPaymentStatus
                    number: 1
                  unapproved:
                    type: UnapprovedPaymentStatus
                    number: 2
              PaymentOutputBranch:
                variants:
                  approved:
                    type: ApprovedPaymentOutput
                    number: 1
                  unapproved:
                    type: UnapprovedPaymentOutput
                    number: 2
            steps:
              - name: "Await Payment Provider"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentRecord"
                outputTypeName: "PaymentStatus"
              - name: "Process Approved Payment Status"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ApprovedPaymentStatus"
                accepts:
                  - "ApprovedPaymentStatus"
                outputTypeName: "ApprovedPaymentOutput"
              - name: "Process Unapproved Payment Status"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "UnapprovedPaymentStatus"
                accepts:
                  - "UnapprovedPaymentStatus"
                outputTypeName: "UnapprovedPaymentOutput"
              - name: "Finalize Payment Output"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PaymentOutputBranch"
                accepts:
                  - "ApprovedPaymentOutput"
                  - "UnapprovedPaymentOutput"
                outputTypeName: "PaymentOutput"
                terminal: true
            """;
        Path configPath = tempDir.resolve("branch-routing-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-branch-routing-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        String approvedProto = Files.readString(outputDir.resolve("process-approved-payment-status-svc.proto"));
        String unapprovedProto = Files.readString(outputDir.resolve("process-unapproved-payment-status-svc.proto"));
        String finalizeProto = Files.readString(outputDir.resolve("finalize-payment-output-svc.proto"));

        assertTrue(
            approvedProto.contains("rpc remoteProcess(ApprovedPaymentStatus) returns (ApprovedPaymentOutput);"));
        assertTrue(
            unapprovedProto.contains("rpc remoteProcess(UnapprovedPaymentStatus) returns (UnapprovedPaymentOutput);"));
        assertTrue(
            finalizeProto.contains("rpc remoteProcess(PaymentOutputBranch) returns (PaymentOutput);"));
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
        assertFalse(Files.exists(outputDir.resolve("external-step-hosts.json")));
        assertFalse(Files.exists(outputDir.resolve("EXTERNAL-STEP-HOSTS.md")));
    }

    @Test
    void generatesExternalStepHostContractPackForRemoteSteps() throws Exception {
        String yaml = """
            version: 2
            appName: "Remote Step Test"
            basePackage: "com.example.remote"
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
        Path configPath = tempDir.resolve("remote-step-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-remote-step-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path manifestPath = outputDir.resolve("external-step-hosts.json");
        Path readmePath = outputDir.resolve("EXTERNAL-STEP-HOSTS.md");

        assertTrue(Files.exists(outputDir.resolve("charge-card-svc.proto")));
        assertTrue(Files.exists(outputDir.resolve("pipeline-types.proto")));
        assertTrue(Files.exists(manifestPath));
        assertTrue(Files.exists(readmePath));

        JsonNode manifest = PipelineJson.mapper().readTree(manifestPath.toFile());
        assertEquals("tpf.external-step-hosts.v1", manifest.path("protocolVersion").asText());
        assertEquals("com.example.remote", manifest.path("basePackage").asText());
        assertEquals("pipeline-types.proto", manifest.path("typesProto").asText());

        JsonNode steps = manifest.path("steps");
        assertEquals(1, steps.size());
        JsonNode step = steps.get(0);
        assertEquals("Charge Card", step.path("step").asText());
        assertEquals("charge-card", step.path("operatorId").asText());
        assertEquals("PROTOBUF_HTTP_V1", step.path("protocol").asText());
        assertEquals("ProcessChargeCardService", step.path("service").asText());
        assertEquals("remoteProcess", step.path("rpc").asText());
        assertEquals("charge-card-svc.proto", step.path("proto").asText());
        assertEquals("ChargeRequest", step.path("inputType").asText());
        assertEquals("ChargeResult", step.path("outputType").asText());
        assertEquals("tpf.remote-operators.charge-card.url", step.path("target").path("urlConfigKey").asText());
        assertEquals("application/x-protobuf", step.path("http").path("contentType").asText());
        assertEquals("google.rpc.Status", step.path("http").path("failureEnvelope").asText());
        JsonNode headers = step.path("http").path("headers");
        assertTrue(headers.isArray(), "headers must be an array");
        assertTrue(headers.size() >= 2, "headers must include at least correlation and execution ids");
        assertEquals("x-tpf-correlation-id", headers.get(0).asText());
        assertEquals("x-tpf-execution-id", headers.get(1).asText());

        String readme = Files.readString(readmePath);
        assertTrue(readme.contains("# External Step Host Contracts"));
        assertTrue(readme.contains("| Charge Card | `charge-card` | `ProcessChargeCardService`"));
        assertTrue(readme.contains("If the result arrives later, model that boundary as an await step"));
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

    @Test
    void generatesExternalStepHostContractPackForMultipleRemoteSteps() throws Exception {
        String yaml = """
            version: 2
            appName: "Multi Remote Step Test"
            basePackage: "com.example.multi"
            transport: "REST"
            messages:
              OrderRequest:
                fields:
                  - number: 1
                    name: "orderId"
                    type: "uuid"
              FraudResult:
                fields:
                  - number: 1
                    name: "riskScore"
                    type: "string"
              ChargeResult:
                fields:
                  - number: 1
                    name: "paymentId"
                    type: "uuid"
            steps:
              - name: "Check Fraud"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "OrderRequest"
                outputTypeName: "FraudResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "fraud-check"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 2000
                  target:
                    urlConfigKey: "tpf.remote-operators.fraud-check.url"
              - name: "Charge Card"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "FraudResult"
                outputTypeName: "ChargeResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "charge-card"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 3000
                  target:
                    urlConfigKey: "tpf.remote-operators.charge-card.url"
            """;
        Path configPath = tempDir.resolve("multi-remote-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-multi-remote-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path manifestPath = outputDir.resolve("external-step-hosts.json");
        assertTrue(Files.exists(manifestPath));
        assertTrue(Files.exists(outputDir.resolve("EXTERNAL-STEP-HOSTS.md")));
        assertTrue(Files.exists(outputDir.resolve("check-fraud-svc.proto")));
        assertTrue(Files.exists(outputDir.resolve("charge-card-svc.proto")));

        JsonNode manifest = PipelineJson.mapper().readTree(manifestPath.toFile());
        JsonNode steps = manifest.path("steps");
        assertEquals(2, steps.size());

        JsonNode first = steps.get(0);
        assertEquals("Check Fraud", first.path("step").asText());
        assertEquals("fraud-check", first.path("operatorId").asText());
        assertEquals("check-fraud-svc.proto", first.path("proto").asText());
        assertEquals("ProcessCheckFraudService", first.path("service").asText());
        assertEquals("OrderRequest", first.path("inputType").asText());
        assertEquals("FraudResult", first.path("outputType").asText());

        JsonNode second = steps.get(1);
        assertEquals("Charge Card", second.path("step").asText());
        assertEquals("charge-card", second.path("operatorId").asText());
        assertEquals("charge-card-svc.proto", second.path("proto").asText());
        assertEquals("ProcessChargeCardService", second.path("service").asText());
        assertEquals("ChargeResult", second.path("outputType").asText());

        String readme = Files.readString(outputDir.resolve("EXTERNAL-STEP-HOSTS.md"));
        assertTrue(readme.contains("Check Fraud"));
        assertTrue(readme.contains("Charge Card"));
    }

    @Test
    void omitsLocalStepsFromExternalStepHostManifest() throws Exception {
        String yaml = """
            version: 2
            appName: "Mixed Steps Test"
            basePackage: "com.example.mixed"
            transport: "REST"
            messages:
              InputMsg:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              LocalResult:
                fields:
                  - number: 1
                    name: "value"
                    type: "string"
              RemoteResult:
                fields:
                  - number: 1
                    name: "output"
                    type: "string"
            steps:
              - name: "Local Step"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "InputMsg"
                outputTypeName: "LocalResult"
              - name: "Remote Step"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "LocalResult"
                outputTypeName: "RemoteResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "remote-op"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 1000
                  target:
                    urlConfigKey: "tpf.remote-operators.remote-op.url"
            """;
        Path configPath = tempDir.resolve("mixed-steps-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-mixed-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path manifestPath = outputDir.resolve("external-step-hosts.json");
        assertTrue(Files.exists(manifestPath));

        JsonNode manifest = PipelineJson.mapper().readTree(manifestPath.toFile());
        JsonNode steps = manifest.path("steps");
        assertEquals(1, steps.size(), "Only remote step should appear in manifest");
        assertEquals("Remote Step", steps.get(0).path("step").asText());
        assertEquals("remote-op", steps.get(0).path("operatorId").asText());
    }

    @Test
    void usesDirectUrlInTargetMapWhenUrlProvided() throws Exception {
        String yaml = """
            version: 2
            appName: "Direct URL Test"
            basePackage: "com.example.directurl"
            transport: "REST"
            messages:
              Input:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              Output:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Call Service"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "Input"
                outputTypeName: "Output"
                execution:
                  mode: "REMOTE"
                  operatorId: "call-service"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 500
                  target:
                    url: "https://service.example.com/grpc"
            """;
        Path configPath = tempDir.resolve("direct-url-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-direct-url-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path manifestPath = outputDir.resolve("external-step-hosts.json");
        assertTrue(Files.exists(manifestPath));

        JsonNode manifest = PipelineJson.mapper().readTree(manifestPath.toFile());
        JsonNode step = manifest.path("steps").get(0);
        assertEquals("https://service.example.com/grpc", step.path("target").path("url").asText());
        assertTrue(step.path("target").path("urlConfigKey").isMissingNode(),
            "urlConfigKey must not appear when direct url is used");
    }

    @Test
    void generatesEnvelopeExternalStepHostContractPolicy() throws Exception {
        String yaml = """
            version: 2
            appName: "Envelope Remote Test"
            basePackage: "com.example.envelope"
            transport: "REST"
            messages:
              ParsedDocument:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              ChunkResult:
                fields:
                  - number: 1
                    name: "status"
                    type: "string"
            steps:
              - name: "Chunk Document"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ParsedDocument"
                outputTypeName: "ChunkResult"
                execution:
                  mode: "REMOTE"
                  operatorId: "chunker"
                  protocol: "ENVELOPE_HTTP_V1"
                  timeoutMs: 500
                  target:
                    url: "https://service.example.com/chunker"
            """;
        Path configPath = tempDir.resolve("envelope-remote-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-envelope-remote-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        JsonNode manifest = PipelineJson.mapper().readTree(outputDir.resolve("external-step-hosts.json").toFile());
        JsonNode step = manifest.path("steps").get(0);
        assertEquals("ENVELOPE_HTTP_V1", step.path("protocol").asText());
        assertEquals("application/vnd.tpf.envelope.v1+json", step.path("http").path("contentType").asText());
        assertEquals("ENVELOPE", step.path("payloadPolicy").path("mode").asText());
        assertEquals("strict", step.path("payloadPolicy").path("control").asText());
        assertEquals("tpf.envelope.v1", step.path("payloadPolicy").path("protocolVersion").asText());
        assertEquals("json", step.path("payloadPolicy").path("payload").get(0).asText());

        String readme = Files.readString(outputDir.resolve("EXTERNAL-STEP-HOSTS.md"));
        assertTrue(readme.contains("For `ENVELOPE_HTTP_V1`"));
        assertTrue(readme.contains("application/vnd.tpf.envelope.v1+json"));
    }

    @Test
    void rejectsRemoteTargetWithBothUrlAndUrlConfigKey() throws Exception {
        String yaml = """
            version: 2
            appName: "Ambiguous Target Test"
            basePackage: "com.example.ambiguous"
            transport: "REST"
            messages:
              Input:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              Output:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Call Service"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "Input"
                outputTypeName: "Output"
                execution:
                  mode: "REMOTE"
                  operatorId: "call-service"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 500
                  target:
                    url: "https://service.example.com/grpc"
                    urlConfigKey: "tpf.remote-operators.call-service.url"
            """;
        Path configPath = tempDir.resolve("ambiguous-target-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-ambiguous-target-out");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new PipelineProtoGenerator().generate(tempDir, configPath, outputDir));
        assertTrue(error.getMessage().contains("Specify either url or urlConfigKey"));
    }

    @Test
    void generatesExternalStepHostContractPackWithGrpcTransport() throws Exception {
        String yaml = """
            version: 2
            appName: "GRPC Remote Test"
            basePackage: "com.example.grpcremote"
            transport: "GRPC"
            messages:
              GrpcInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              GrpcOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Grpc Remote"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "GrpcInput"
                outputTypeName: "GrpcOutput"
                execution:
                  mode: "REMOTE"
                  operatorId: "grpc-remote"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 1000
                  target:
                    urlConfigKey: "tpf.remote-operators.grpc-remote.url"
            """;
        Path configPath = tempDir.resolve("grpc-remote-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-grpc-remote-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        assertTrue(Files.exists(outputDir.resolve("external-step-hosts.json")),
            "Contract pack must be emitted even when transport is GRPC");
        assertTrue(Files.exists(outputDir.resolve("EXTERNAL-STEP-HOSTS.md")));
        assertTrue(Files.exists(outputDir.resolve("orchestrator.proto")),
            "GRPC transport still generates orchestrator proto");

        JsonNode manifest = PipelineJson.mapper().readTree(outputDir.resolve("external-step-hosts.json").toFile());
        assertEquals(1, manifest.path("steps").size());
        assertEquals("grpc-remote", manifest.path("steps").get(0).path("operatorId").asText());
    }

    @Test
    void externalStepHostManifestContainsAllRequiredHttpHeaders() throws Exception {
        String yaml = """
            version: 2
            appName: "Header Test"
            basePackage: "com.example.headers"
            transport: "REST"
            messages:
              Req:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              Res:
                fields:
                  - number: 1
                    name: "value"
                    type: "string"
            steps:
              - name: "Step One"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "Req"
                outputTypeName: "Res"
                execution:
                  mode: "REMOTE"
                  operatorId: "step-one"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 1000
                  target:
                    urlConfigKey: "tpf.remote-operators.step-one.url"
            """;
        Path configPath = tempDir.resolve("headers-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-headers-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        JsonNode manifest = PipelineJson.mapper().readTree(outputDir.resolve("external-step-hosts.json").toFile());
        JsonNode headers = manifest.path("steps").get(0).path("http").path("headers");
        assertTrue(headers.isArray());
        List<String> headerList = new ArrayList<>();
        headers.forEach(h -> headerList.add(h.asText()));

        assertTrue(headerList.contains("x-tpf-correlation-id"), "must include x-tpf-correlation-id");
        assertTrue(headerList.contains("x-tpf-execution-id"), "must include x-tpf-execution-id");
        assertTrue(headerList.contains("x-tpf-idempotency-key"), "must include x-tpf-idempotency-key");
        assertTrue(headerList.contains("x-tpf-retry-attempt"), "must include x-tpf-retry-attempt");
        assertTrue(headerList.contains("x-tpf-deadline-epoch-ms"), "must include x-tpf-deadline-epoch-ms");
        assertTrue(headerList.contains("x-tpf-dispatch-ts-epoch-ms"), "must include x-tpf-dispatch-ts-epoch-ms");
        assertTrue(headerList.contains("x-tpf-parent-item-id"), "must include x-tpf-parent-item-id");
        assertEquals(7, headerList.size(), "exactly 7 TPF metadata headers must be declared");

        JsonNode http = manifest.path("steps").get(0).path("http");
        assertEquals("POST", http.path("method").asText());
        assertEquals("application/x-protobuf", http.path("accept").asText());
        assertEquals("2xx", http.path("successStatus").asText());
    }

    @Test
    void readmeTableEscapesPipeCharacterInStepName() throws Exception {
        String yaml = """
            version: 2
            appName: "Pipe Escape Test"
            basePackage: "com.example.pipe"
            transport: "REST"
            messages:
              PipeInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              PipeOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Step A | Step B"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "PipeInput"
                outputTypeName: "PipeOutput"
                execution:
                  mode: "REMOTE"
                  operatorId: "pipe-step"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 500
                  target:
                    urlConfigKey: "tpf.remote-operators.pipe-step.url"
            """;
        Path configPath = tempDir.resolve("pipe-escape-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-pipe-escape-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        String readme = Files.readString(outputDir.resolve("EXTERNAL-STEP-HOSTS.md"));
        assertTrue(readme.contains("Step A \\| Step B"),
            "Pipe characters in step names must be escaped as \\| in readme table");
        assertFalse(readme.contains("| Step A | Step B |"),
            "Unescaped pipe in step name would break markdown table");
    }

    @Test
    void omitsTimeoutFromContractWhenNotSpecified() throws Exception {
        String yaml = """
            version: 2
            appName: "No Timeout Test"
            basePackage: "com.example.notimeout"
            transport: "REST"
            messages:
              NtInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              NtOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "No Timeout Step"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "NtInput"
                outputTypeName: "NtOutput"
                execution:
                  mode: "REMOTE"
                  operatorId: "no-timeout-op"
                  protocol: "PROTOBUF_HTTP_V1"
                  target:
                    urlConfigKey: "tpf.remote-operators.no-timeout-op.url"
            """;
        Path configPath = tempDir.resolve("no-timeout-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-no-timeout-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path manifestPath = outputDir.resolve("external-step-hosts.json");
        assertTrue(Files.exists(manifestPath));

        JsonNode manifest = PipelineJson.mapper().readTree(manifestPath.toFile());
        JsonNode step = manifest.path("steps").get(0);
        assertEquals("No Timeout Step", step.path("step").asText());
        assertTrue(step.path("timeoutMs").isNull() || step.path("timeoutMs").isMissingNode(),
            "timeoutMs must be absent or null when not configured");
    }

    @Test
    void externalStepHostManifestUsesCustomTypesProtoName() throws Exception {
        String yaml = """
            version: 2
            appName: "Custom Types Proto Test"
            basePackage: "com.example.custom"
            transport: "REST"
            messages:
              CtInput:
                fields:
                  - number: 1
                    name: "id"
                    type: "uuid"
              CtOutput:
                fields:
                  - number: 1
                    name: "result"
                    type: "string"
            steps:
              - name: "Custom Types Step"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "CtInput"
                outputTypeName: "CtOutput"
                execution:
                  mode: "REMOTE"
                  operatorId: "custom-types-op"
                  protocol: "PROTOBUF_HTTP_V1"
                  timeoutMs: 1000
                  target:
                    urlConfigKey: "tpf.remote-operators.custom-types-op.url"
            """;
        Path configPath = tempDir.resolve("custom-types-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-custom-types-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir, "my-shared-types.proto");

        Path manifestPath = outputDir.resolve("external-step-hosts.json");
        assertTrue(Files.exists(manifestPath));
        assertTrue(Files.exists(outputDir.resolve("my-shared-types.proto")));

        JsonNode manifest = PipelineJson.mapper().readTree(manifestPath.toFile());
        assertEquals("my-shared-types.proto", manifest.path("typesProto").asText(),
            "manifest typesProto must reflect the custom types proto name");

        JsonNode step = manifest.path("steps").get(0);
        assertEquals("my-shared-types.proto", step.path("typesProto").asText(),
            "per-step typesProto must match the custom types proto name");

        String readme = Files.readString(outputDir.resolve("EXTERNAL-STEP-HOSTS.md"));
        assertTrue(readme.contains("`my-shared-types.proto`"),
            "Readme shared types proto line must use the custom name");
    }

    @Test
    void generatesPayloadReferenceTypeForPayloadRefFields() throws Exception {
        String yaml = """
            version: 2
            appName: "Materialized Search"
            basePackage: "com.example.search"
            transport: "GRPC"
            messages:
              ParsedDocument:
                fields:
                  - number: 1
                    name: "docId"
                    type: "string"
                  - number: 2
                    name: "text"
                    type: "string"
                    optional: true
                    referenceable:
                      refField: "textRef"
                  - number: 3
                    name: "textRef"
                    type: "payload_ref"
                    optional: true
            steps:
              - name: "Pass Document"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "ParsedDocument"
                outputTypeName: "ParsedDocument"
            """;
        Path configPath = tempDir.resolve("pipeline-config-payload-ref.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("proto-payload-ref-out");

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        String typesContent = Files.readString(outputDir.resolve("pipeline-types.proto"));
        assertTrue(typesContent.contains("message PayloadReference {"));
        assertTrue(typesContent.contains("string provider = 1;"));
        assertTrue(typesContent.contains("map<string, string> metadata = 9;"));
        assertTrue(typesContent.contains("PayloadReference textRef = 3;"));
        assertFalse(typesContent.contains("optional PayloadReference textRef = 3;"));
    }
}
