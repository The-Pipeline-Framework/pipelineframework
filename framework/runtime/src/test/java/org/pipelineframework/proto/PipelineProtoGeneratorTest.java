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

import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.tools.ToolProvider;

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
    void conciseAndVerboseTemplatesGenerateIdenticalArtifacts() throws Exception {
        String header = """
            version: 2
            appName: "Equivalent"
            basePackage: "com.example.equivalent"
            transport: "GRPC"
            """;
        Path verboseDir = Files.createDirectories(tempDir.resolve("verbose"));
        Path conciseDir = Files.createDirectories(tempDir.resolve("concise"));
        Path verboseConfig = verboseDir.resolve("pipeline.yaml");
        Path conciseConfig = conciseDir.resolve("pipeline.yaml");
        Files.writeString(verboseConfig, header + """
            messages:
              PaymentRequest:
                fields:
                  - number: 1
                    name: orderId
                    type: uuid
              ValidatedPayment:
                fields:
                  - number: 1
                    name: orderId
                    type: uuid
              PaymentOutcome:
                fields:
                  - number: 1
                    name: orderId
                    type: uuid
            steps:
              - name: Validate Payment
                cardinality: ONE_TO_ONE
                inputTypeName: PaymentRequest
                outputTypeName: ValidatedPayment
              - name: Process Payment
                cardinality: ONE_TO_ONE
                inputTypeName: ValidatedPayment
                outputTypeName: PaymentOutcome
            """);
        Files.writeString(conciseConfig, header + """
            types:
              PaymentRequest:
                fields: [[1, orderId, uuid]]
              ValidatedPayment:
                fields: [[1, orderId, uuid]]
              PaymentOutcome:
                fields: [[1, orderId, uuid]]
            contract:
              input: PaymentRequest
              output: PaymentOutcome
            steps:
              - name: Validate Payment
                cardinality: ONE_TO_ONE
                output: ValidatedPayment
              - name: Process Payment
                cardinality: ONE_TO_ONE
                output: PaymentOutcome
            """);
        Path verboseOutput = verboseDir.resolve("generated");
        Path conciseOutput = conciseDir.resolve("generated");

        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(verboseDir, verboseConfig, verboseOutput);
        generator.generate(conciseDir, conciseConfig, conciseOutput);

        assertEquals(readGeneratedFiles(verboseOutput), readGeneratedFiles(conciseOutput));
    }

    @Test
    void generatesV3AlgebraicTypesAndUsesThemFromStepContracts() throws Exception {
        Path configPath = tempDir.resolve("pipeline.yaml");
        Path outputDir = tempDir.resolve("generated-v3");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              OrderId:
                wraps: uuid
              CustomerId:
                wraps: uuid
              Description:
                alias: string
              PaymentInput:
                alias: PaymentRecord
              PaymentRecord:
                fields:
                  - [description, Description]
                  - [orderId, OrderId]
                  - [paymentId, uuid]
                  - [reference, payload_ref]
              PaymentOutcome:
                variants:
                  approved: PaymentRecord
                  requiresReview: PaymentRecord
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: PaymentInput
                output: PaymentOutcome
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }

        String types = Files.readString(outputDir.resolve("pipeline-types.proto"));
        assertTrue(types.contains("message OrderId {\n  optional string value = 1;\n}"));
        assertTrue(types.contains("message CustomerId {\n  optional string value = 1;\n}"));
        assertFalse(types.contains("message Description"));
        assertFalse(types.contains("message PaymentInput"));
        assertTrue(types.contains("optional string description = 1;"));
        assertTrue(types.contains("OrderId order_id = 2;"));
        assertTrue(types.contains("optional string payment_id = 3;"));
        assertTrue(types.contains("PayloadReference reference = 4;"));
        assertTrue(types.contains("oneof value {"));
        assertTrue(types.contains("PaymentRecord approved = 1;"));
        assertTrue(types.contains("PaymentRecord requires_review = 2;"));
        assertFalse(types.contains("optional PaymentRecord approved"));

        String step = Files.readString(outputDir.resolve("process-payment-svc.proto"));
        String orchestrator = Files.readString(outputDir.resolve("orchestrator.proto"));
        assertTrue(step.contains("import \"pipeline-types.proto\";"));
        assertTrue(step.contains("rpc remoteProcess(PaymentRecord) returns (PaymentOutcome);"));
        assertTrue(orchestrator.contains("rpc Run (PaymentRecord) returns (PaymentOutcome);"));

        String state = Files.readString(tempDir.resolve("pipeline.idl.json"));
        assertTrue(state.contains("\"protoName\" : \"payment_id\""));
        assertTrue(state.contains("\"protoName\" : \"requires_review\""));
    }

    @Test
    void generatesV3JavaNominalRecordsAndAdaptersInDeclaredFieldOrder() throws Exception {
        Path configPath = tempDir.resolve("pipeline.yaml");
        Path outputDir = tempDir.resolve("generated-v3-java");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              OrderId:
                wraps: uuid
              CustomerId:
                wraps: uuid
              DisplayName:
                alias: string
              PaymentRecord:
                fields:
                  - [customerId, CustomerId]
                  - [orderId, OrderId]
                  - [displayName, DisplayName]
                  - [reference, payload_ref]
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: PaymentRecord
                output: PaymentRecord
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
            new PipelineV3JavaDomainGenerator().generate(tempDir, configPath, outputDir);
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }

        Path domain = outputDir.resolve("com/example/payments/domain");
        String record = Files.readString(domain.resolve("PaymentRecord.java"));
        String adapters = Files.readString(domain.resolve("PipelineDomainProtoAdapters.java"));
        assertTrue(record.indexOf("CustomerId customerId") < record.indexOf("OrderId orderId"));
        assertTrue(record.indexOf("OrderId orderId") < record.indexOf("String displayName"));
        assertTrue(record.contains("org.pipelineframework.repository.PayloadReference reference"));
        assertTrue(Files.exists(domain.resolve("OrderId.java")));
        assertTrue(Files.exists(domain.resolve("CustomerId.java")));
        assertFalse(Files.exists(domain.resolve("DisplayName.java")));
        assertTrue(adapters.contains("public static com.example.payments.grpc.PipelineTypes.PaymentRecord toProto"));
        assertTrue(adapters.contains("toProtoPayloadReference"));
        assertTrue(adapters.contains("This surface is intentionally provisional"));
    }

    @Test
    void rendersV3WrapperConstraintsAsConstructorInvariants() throws Exception {
        Path configPath = tempDir.resolve("pipeline-constraints.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: "Constraints"
            basePackage: "com.example.constraints"
            transport: "GRPC"
            types:
              CurrencyCode:
                wraps: string
                minLength: 3
                maxLength: 3
                pattern: "[A-Z]{3}"
              ContactEmail:
                wraps: string
                format: email
              PositiveRatio:
                wraps: float64
                minimumExclusive: 0
                maximum: 1
            steps:
              - name: Process Constraints
                cardinality: ONE_TO_ONE
                input: CurrencyCode
                output: CurrencyCode
            """);
        var config = new org.pipelineframework.config.template.PipelineTemplateConfigLoader().load(configPath);
        var plan = new PipelineV3GenerationPlan(config.basePackage(), config.typeModel(),
            org.pipelineframework.config.template.PipelineIdlSnapshot.from(config));
        List<PipelineJavaDomainRenderer.RenderedSource> sources = new PipelineJavaDomainRenderer().render(plan);
        String currency = sources.stream().filter(source -> source.relativePath().getFileName().toString().equals("CurrencyCode.java"))
            .findFirst().orElseThrow().content();
        String ratio = sources.stream().filter(source -> source.relativePath().getFileName().toString().equals("PositiveRatio.java"))
            .findFirst().orElseThrow().content();
        String validation = sources.stream().filter(source -> source.relativePath().getFileName().toString().equals("PipelineDomainValidation.java"))
            .findFirst().orElseThrow().content();
        String adapters = sources.stream().filter(source -> source.relativePath().getFileName().toString().equals("PipelineDomainProtoAdapters.java"))
            .findFirst().orElseThrow().content();

        assertTrue(currency.contains("if (value == null) { throw new IllegalArgumentException"));
        assertTrue(currency.contains("validateString(\"CurrencyCode\", value"));
        assertTrue(ratio.contains("validateFloat64(\"PositiveRatio\", value"));
        assertTrue(validation.contains("codePointCount"));
        assertTrue(validation.contains("matcher(value).matches()"));
        assertTrue(validation.contains("Float.toString(value)"));
        assertTrue(validation.contains("Double.toString(value)"));
        assertTrue(validation.contains("isPracticalEmail"));
        assertFalse(validation.contains("new java.math.BigDecimal(value)"));
        assertTrue(adapters.contains("builder.setValue(value.value());"));
        assertFalse(adapters.contains("if (value.value() != null)"));
        assertTrue(adapters.contains("return new CurrencyCode(value.hasValue() ? value.getValue() : null);"));

        Path sourceRoot = tempDir.resolve("constrained-domain-sources");
        for (PipelineJavaDomainRenderer.RenderedSource source : sources) {
            if (source.relativePath().getFileName().toString().equals("PipelineDomainProtoAdapters.java")) {
                continue;
            }
            Path target = sourceRoot.resolve(source.relativePath());
            Files.createDirectories(target.getParent());
            Files.writeString(target, source.content());
        }
        Path classes = tempDir.resolve("constrained-domain-classes");
        try (var files = Files.walk(sourceRoot)) {
            List<String> generatedSources = files.filter(path -> path.toString().endsWith(".java")).map(Path::toString).toList();
            List<String> compilerArguments = new ArrayList<>(List.of("-d", classes.toString()));
            compilerArguments.addAll(generatedSources);
            assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null,
                compilerArguments.toArray(String[]::new)));
        }
        try (URLClassLoader loader = URLClassLoader.newInstance(new java.net.URL[] { classes.toUri().toURL() })) {
            Class<?> currencyClass = loader.loadClass("com.example.constraints.domain.CurrencyCode");
            Class<?> email = loader.loadClass("com.example.constraints.domain.ContactEmail");
            Class<?> ratioClass = loader.loadClass("com.example.constraints.domain.PositiveRatio");
            assertNotNull(currencyClass.getConstructor(String.class).newInstance("USD"));
            assertNotNull(email.getConstructor(String.class).newInstance("person@example.com"));
            assertNotNull(ratioClass.getConstructor(Double.class).newInstance(0.5d));
            assertWrapperConstructionFails(currencyClass, "usd");
            assertWrapperConstructionFails(currencyClass, (Object) null);
            assertWrapperConstructionFails(email, "person@.example");
            assertWrapperConstructionFails(ratioClass, Double.NaN);
        }
    }

    @Test
    void generatesV3BytesAsByteStringWithoutArrayConversions() throws Exception {
        Path configPath = tempDir.resolve("pipeline-bytes.yaml");
        Path outputDir = tempDir.resolve("generated-v3-bytes");
        Files.writeString(configPath, """
            version: 3
            appName: "Binary Values"
            basePackage: "com.example.binary"
            transport: "GRPC"
            types:
              BinaryValue:
                wraps: bytes
              BinaryRecord:
                fields: [[rawContent, bytes], [value, BinaryValue]]
              BinaryOutcome:
                variants:
                  accepted: BinaryValue
            steps:
              - name: Process Binary Value
                cardinality: ONE_TO_ONE
                input: BinaryRecord
                output: BinaryOutcome
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
            new PipelineV3JavaDomainGenerator().generate(tempDir, configPath, outputDir);
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }

        Path domain = outputDir.resolve("com/example/binary/domain");
        String record = Files.readString(domain.resolve("BinaryRecord.java"));
        String wrapper = Files.readString(domain.resolve("BinaryValue.java"));
        String union = Files.readString(domain.resolve("BinaryOutcome.java"));
        String adapters = Files.readString(domain.resolve("PipelineDomainProtoAdapters.java"));
        assertTrue(record.contains("com.google.protobuf.ByteString rawContent"));
        assertTrue(wrapper.contains("com.google.protobuf.ByteString value"));
        assertTrue(union.contains("record Accepted(BinaryValue value) implements BinaryOutcome"));
        assertTrue(adapters.contains("builder.setRawContent(value.rawContent());"));
        assertTrue(adapters.contains("value.hasRawContent() ? value.getRawContent() : null"));
        assertFalse(adapters.contains("ByteString.copyFrom"));
        assertFalse(adapters.contains(".toByteArray()"));
    }

    @Test
    void generatesV3JavaSealedUnionsAndProtobufAdapters() throws Exception {
        Path configPath = tempDir.resolve("pipeline.yaml");
        Path outputDir = tempDir.resolve("generated-v3-union");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              Approved:
                fields: [[id, uuid]]
              Outcome:
                variants:
                  approved: Approved
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: Approved
                output: Outcome
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
            new PipelineV3JavaDomainGenerator().generate(tempDir, configPath, outputDir);
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }

        assertTrue(Files.exists(outputDir.resolve("pipeline-types.proto")));
        String union = Files.readString(outputDir.resolve("com/example/payments/domain/Outcome.java"));
        String adapters = Files.readString(outputDir.resolve("com/example/payments/domain/PipelineDomainProtoAdapters.java"));
        assertTrue(union.contains("public sealed interface Outcome permits Outcome.Approved"));
        assertTrue(union.contains("record Approved(Approved value) implements Outcome"));
        assertTrue(union.contains("return \"approved\""));
        assertTrue(adapters.contains("setApproved(toProto(variant.value()))"));
        assertTrue(adapters.contains("case APPROVED -> { return new Outcome.Approved(fromProto(value.getApproved())); }"));
    }

    @Test
    void rejectsV3JavaIdentifiersThatCannotBeRepresentedWithoutChangingTheDslName() throws Exception {
        Path configPath = tempDir.resolve("pipeline.yaml");
        Path outputDir = tempDir.resolve("generated-v3-invalid-java");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              PaymentRecord:
                fields: [[class, string]]
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: PaymentRecord
                output: PaymentRecord
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
            IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new PipelineV3JavaDomainGenerator().generate(tempDir, configPath, outputDir));
            assertTrue(error.getMessage().contains("cannot represent field 'PaymentRecord.class'"));
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }
    }

    @Test
    void rejectsV3JavaContextualTypeIdentifiers() throws Exception {
        Path configPath = tempDir.resolve("pipeline-contextual-name.yaml");
        Path outputDir = tempDir.resolve("generated-v3-contextual-name");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              record:
                fields: [[id, string]]
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: record
                output: record
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
            IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new PipelineV3JavaDomainGenerator().generate(tempDir, configPath, outputDir));
            assertTrue(error.getMessage().contains("cannot represent type 'record'"));
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }
    }

    @Test
    void generatedV3JavaAdaptersCompileAndRoundTripNominalRecords() throws Exception {
        Path configPath = tempDir.resolve("pipeline.yaml");
        Path outputDir = tempDir.resolve("generated-v3-roundtrip");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              OrderId:
                wraps: uuid
              PaymentRecord:
                fields:
                  - [id, OrderId]
                  - [note, string]
              PaymentOutcome:
                variants:
                  approved: PaymentRecord
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: PaymentRecord
                output: PaymentOutcome
            """);
        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
            new PipelineV3JavaDomainGenerator().generate(tempDir, configPath, outputDir);
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }
        Path stub = outputDir.resolve("com/example/payments/grpc/PipelineTypes.java");
        Files.createDirectories(stub.getParent());
        Files.writeString(stub, """
            package com.example.payments.grpc;
            public final class PipelineTypes {
              public static final class OrderId {
                private final String value;
                private OrderId(String value) { this.value = value; }
                public static Builder newBuilder() { return new Builder(); }
                public boolean hasValue() { return value != null; }
                public String getValue() { return value; }
                public static final class Builder {
                  private String value;
                  public Builder setValue(String value) { this.value = value; return this; }
                  public OrderId build() { return new OrderId(value); }
                }
              }
              public static final class PaymentRecord {
                private final OrderId id; private final String note;
                private PaymentRecord(OrderId id, String note) { this.id = id; this.note = note; }
                public static Builder newBuilder() { return new Builder(); }
                public boolean hasId() { return id != null; }
                public OrderId getId() { return id; }
                public boolean hasNote() { return note != null; }
                public String getNote() { return note; }
                public static final class Builder {
                  private OrderId id; private String note;
                  public Builder setId(OrderId id) { this.id = id; return this; }
                  public Builder setNote(String note) { this.note = note; return this; }
                  public PaymentRecord build() { return new PaymentRecord(id, note); }
                }
              }
              public static final class PaymentOutcome {
                public enum ValueCase { APPROVED, VALUE_NOT_SET }
                private final PaymentRecord approved;
                private PaymentOutcome(PaymentRecord approved) { this.approved = approved; }
                public static Builder newBuilder() { return new Builder(); }
                public ValueCase getValueCase() { return approved == null ? ValueCase.VALUE_NOT_SET : ValueCase.APPROVED; }
                public PaymentRecord getApproved() { return approved; }
                public static final class Builder {
                  private PaymentRecord approved;
                  public Builder setApproved(PaymentRecord approved) { this.approved = approved; return this; }
                  public PaymentOutcome build() { return new PaymentOutcome(approved); }
                }
              }
            }
            """);
        Path classes = tempDir.resolve("compiled-domain");
        List<String> sources;
        try (var files = Files.walk(outputDir)) {
            sources = files.filter(path -> path.toString().endsWith(".java")).map(Path::toString).toList();
        }
        List<String> compilerArguments = new ArrayList<>();
        compilerArguments.add("-d");
        compilerArguments.add(classes.toString());
        compilerArguments.addAll(sources);
        assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null,
            compilerArguments.toArray(String[]::new)));
        try (URLClassLoader loader = URLClassLoader.newInstance(new java.net.URL[] { classes.toUri().toURL() })) {
            Class<?> orderId = loader.loadClass("com.example.payments.domain.OrderId");
            Class<?> record = loader.loadClass("com.example.payments.domain.PaymentRecord");
            Class<?> outcome = loader.loadClass("com.example.payments.domain.PaymentOutcome");
            Class<?> approved = loader.loadClass("com.example.payments.domain.PaymentOutcome$Approved");
            Class<?> adapters = loader.loadClass("com.example.payments.domain.PipelineDomainProtoAdapters");
            Object id = orderId.getConstructor(UUID.class).newInstance(UUID.fromString("d7f7c765-38b3-4ca8-b33b-f4b3af6983aa"));
            Object input = record.getConstructor(orderId, String.class).newInstance(id, "memo");
            Object proto = adapters.getMethod("toProto", record).invoke(null, input);
            Object roundTripped = adapters.getMethod("fromProto", proto.getClass()).invoke(null, proto);
            assertEquals(input, roundTripped);
            Object unionValue = approved.getConstructor(record).newInstance(input);
            Object unionProto = adapters.getMethod("toProto", outcome).invoke(null, unionValue);
            Object unionRoundTripped = adapters.getMethod("fromProto", unionProto.getClass()).invoke(null, unionProto);
            assertEquals(unionValue, unionRoundTripped);
            assertEquals("approved", outcome.getMethod("discriminator").invoke(unionRoundTripped));
        }
    }

    @Test
    void rejectsV3StepContractsThatResolveToScalars() throws Exception {
        Path configPath = tempDir.resolve("scalar-contract.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: "Payments"
            basePackage: "com.example.payments"
            transport: "GRPC"
            types:
              PaymentId:
                alias: uuid
              PaymentOutcome:
                fields: [[id, uuid]]
            steps:
              - name: Process Payment
                cardinality: ONE_TO_ONE
                input: PaymentId
                output: PaymentOutcome
            """);

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> new PipelineProtoGenerator().generate(tempDir, configPath, tempDir.resolve("generated-scalar")));
            assertTrue(error.getMessage().contains("resolves to a scalar"));
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }
    }

    private Map<String, String> readGeneratedFiles(Path root) throws Exception {
        Map<String, String> files = new TreeMap<>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                files.put(root.relativize(path).toString(), Files.readString(path));
            }
        }
        return files;
    }

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
    void createsAConfigSpecificIdlLockForTagFreeVariantTemplates() throws Exception {
        Path configPath = tempDir.resolve("pipeline.object-ingest.yaml");
        Path outputDir = tempDir.resolve("proto-idl-lock-out");
        Files.writeString(configPath, """
            version: 2
            appName: "Variant Lock"
            basePackage: "com.example.lock"
            transport: "GRPC"
            types:
              Input:
                fields: [[requestId, uuid]]
            steps: []
            """);

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);

        Path lockPath = tempDir.resolve("pipeline.object-ingest.idl.json");
        assertTrue(Files.exists(lockPath));
        Files.writeString(configPath, """
            version: 2
            appName: "Variant Lock"
            basePackage: "com.example.lock"
            transport: "GRPC"
            types:
              Input:
                fields: [[requestId, uuid]]
            steps: []
            """);

        new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
        assertTrue(Files.readString(lockPath).contains("\"requestId\""));
    }

    @Test
    void requiresCommittedIdlStateWhenConfigured() throws Exception {
        Path configPath = tempDir.resolve("pipeline.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Strict Lock"
            basePackage: "com.example.strict"
            transport: "GRPC"
            types:
              Input:
                fields: [[requestId, uuid]]
            steps: []
            """);

        System.setProperty("pipeline.idl.require-committed-state", "true");
        try {
            IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new PipelineProtoGenerator().generate(tempDir, configPath, tempDir.resolve("strict-lock-out")));
            assertTrue(exception.getMessage().contains("missing committed IDL state"));
        } finally {
            System.clearProperty("pipeline.idl.require-committed-state");
        }
    }

    @Test
    void rejectsLegacyJavaOnlyV2ContractsBeforeGeneratingANullProtoType() throws Exception {
        Path configPath = tempDir.resolve("pipeline-config-v2-java-only.yaml");
        Files.writeString(configPath, """
            version: 2
            appName: "Java Only"
            basePackage: "com.example"
            transport: "GRPC"
            steps:
              - name: "Payment"
                cardinality: "ONE_TO_ONE"
                input: "com.example.domain.PaymentRecord"
                output: "com.example.domain.PaymentStatus"
            """);

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> new PipelineProtoGenerator().generate(tempDir, configPath, tempDir.resolve("proto-java-only-out")));

        assertTrue(error.getMessage().contains("requires a declared logical input contract"));
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

    @Test
    void requiresCommittedIdlStateBeforeWritingV3ProtoArtifacts() throws Exception {
        Path configPath = tempDir.resolve("v3-pipeline.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Payment
                output: Payment
            """);
        Path outputDir = tempDir.resolve("v3-proto");

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> new PipelineProtoGenerator().generate(tempDir, configPath, outputDir));

        assertTrue(exception.getMessage().contains("require committed pipeline.idl.json"));
        assertFalse(Files.exists(outputDir));
    }

    @Test
    @ClearSystemProperty(key = "pipeline.idl.bootstrap")
    @ClearSystemProperty(key = "pipeline.idl.require-committed-state")
    void bootstrapsV3IdlStateOnlyWhenExplicitlyEnabled() throws Exception {
        Path configPath = tempDir.resolve("v3-bootstrap-pipeline.yaml");
        Files.writeString(configPath, """
            version: 3
            appName: V3 Bootstrap
            basePackage: com.example.v3
            transport: GRPC
            types:
              Payment:
                fields: [[id, uuid]]
            steps: []
            """);
        Path outputDir = tempDir.resolve("v3-bootstrap-proto");
        Path lockPath = tempDir.resolve("v3-bootstrap-pipeline.idl.json");

        System.setProperty("pipeline.idl.bootstrap", "true");
        try {
            new PipelineProtoGenerator().generate(tempDir, configPath, outputDir);
        } finally {
            System.clearProperty("pipeline.idl.bootstrap");
        }

        assertTrue(Files.exists(lockPath));
        assertTrue(Files.exists(outputDir.resolve("pipeline-types.proto")));
    }

    @Test
    void parsesV3ContractGeneratorArgumentsStrictly() {
        PipelineV3ContractGenerator.Arguments arguments = PipelineV3ContractGenerator.Arguments.parse(new String[] {
            "--module-dir", "module", "--config=config/pipeline.yaml", "--output-dir", "generated"
        });

        assertEquals(Path.of("module"), arguments.moduleDir());
        assertEquals(Path.of("config/pipeline.yaml"), arguments.configPath());
        assertEquals(Path.of("generated"), arguments.outputDir());
        assertFalse(arguments.help());
        assertTrue(PipelineV3ContractGenerator.Arguments.parse(new String[] { "--help" }).help());
        assertThrows(IllegalArgumentException.class,
            () -> PipelineV3ContractGenerator.Arguments.parse(new String[] { "--config", "--output-dir" }));
        assertThrows(IllegalArgumentException.class,
            () -> PipelineV3ContractGenerator.Arguments.parse(new String[] { "--output-dir=" }));
        assertThrows(IllegalArgumentException.class,
            () -> PipelineV3ContractGenerator.Arguments.parse(new String[] { "--unknown" }));
    }

    private void assertWrapperConstructionFails(Class<?> wrapper, Object... arguments) throws Exception {
        java.lang.reflect.Constructor<?> constructor = java.util.Arrays.stream(wrapper.getConstructors())
            .filter(candidate -> candidate.getParameterCount() == arguments.length)
            .findFirst().orElseThrow();
        java.lang.reflect.InvocationTargetException exception = assertThrows(java.lang.reflect.InvocationTargetException.class,
            () -> constructor.newInstance(arguments));
        assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    }
}
