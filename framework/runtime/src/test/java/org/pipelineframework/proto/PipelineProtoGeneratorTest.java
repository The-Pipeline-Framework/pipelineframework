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

import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void handlesEmptyStepsGracefully() throws Exception {
        String yaml = """
            appName: "Empty App"
            basePackage: "com.example.empty"
            transport: "GRPC"
            steps: []
            """;
        Path configPath = tempDir.resolve("empty-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("empty-proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        assertTrue(Files.exists(outputDir));
        assertTrue(Files.list(outputDir).count() == 0);
    }

    @Test
    void skipsOrchestratorProtoWhenTransportIsRest() throws Exception {
        String yaml = """
            appName: "REST App"
            basePackage: "com.example.rest"
            transport: "REST"
            steps:
              - name: "Process Data"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "DataInput"
                inputFields:
                  - name: "id"
                    type: "String"
                    protoType: "string"
                outputTypeName: "DataOutput"
                outputFields:
                  - name: "status"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("rest-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("rest-proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path stepProtoPath = outputDir.resolve("process-data-svc.proto");
        Path orchestratorProtoPath = outputDir.resolve("orchestrator.proto");

        assertTrue(Files.exists(stepProtoPath));
        assertTrue(!Files.exists(orchestratorProtoPath));
    }

    @Test
    void throwsWhenBasePackageIsMissing() throws Exception {
        String yaml = """
            appName: "No Package App"
            transport: "GRPC"
            steps:
              - name: "Process"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "In"
                outputTypeName: "Out"
            """;
        Path configPath = tempDir.resolve("no-package-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("no-package-proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        IllegalStateException error = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalStateException.class,
            () -> generator.generate(tempDir, configPath, outputDir));
        assertTrue(error.getMessage().contains("missing basePackage"));
    }

    @Test
    void handlesMapAndListFieldTypes() throws Exception {
        String yaml = """
            appName: "Collection Types App"
            basePackage: "com.example.collections"
            transport: "GRPC"
            steps:
              - name: "Process Collections"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "CollectionInput"
                inputFields:
                  - name: "tags"
                    type: "List<String>"
                    protoType: "string"
                  - name: "metadata"
                    type: "Map<String, String>"
                    protoType: "string"
                outputTypeName: "CollectionOutput"
                outputFields:
                  - name: "result"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("collections-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("collections-proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path protoPath = outputDir.resolve("process-collections-svc.proto");
        assertTrue(Files.exists(protoPath));

        String proto = Files.readString(protoPath);
        assertTrue(proto.contains("repeated string tags = 1;"));
        assertTrue(proto.contains("map<string, string> metadata = 2;"));
    }

    @Test
    void rendersBeforeStepAspects() throws Exception {
        String yaml = """
            appName: "Aspect Test"
            basePackage: "com.example.aspects"
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
              - name: "Process Data"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "DataInput"
                inputFields:
                  - name: "value"
                    type: "String"
                    protoType: "string"
                outputTypeName: "DataOutput"
                outputFields:
                  - name: "result"
                    type: "String"
                    protoType: "string"
            """;
        Path configPath = tempDir.resolve("aspects-config.yaml");
        Files.writeString(configPath, yaml);
        Path outputDir = tempDir.resolve("aspects-proto-out");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(tempDir, configPath, outputDir);

        Path protoPath = outputDir.resolve("process-data-svc.proto");
        String proto = Files.readString(protoPath);
        assertTrue(proto.contains("service ObserveValidationDataInputSideEffectService"));
        assertTrue(proto.contains("service ObserveValidationDataOutputSideEffectService"));
    }
}