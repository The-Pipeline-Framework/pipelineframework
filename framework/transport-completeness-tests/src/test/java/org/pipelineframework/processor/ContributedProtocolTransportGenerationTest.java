package org.pipelineframework.processor;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import com.google.protobuf.DescriptorProtos;
import java.io.IOException;
import java.net.URLClassLoader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.tools.ToolProvider;
import javax.tools.StandardLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorMatch;
import org.pipelineframework.connector.vector.VectorSearchResult;

class ContributedProtocolTransportGenerationTest {
    @TempDir
    Path tempDir;

    @Test
    void restGeneratesCompilerOwnedDtosAndTypedMappersForContributedRecords() throws Exception {
        Fixture fixture = compile("REST", "COMPUTE", """
            contract:
              input: <tpf.embedding.EmbeddingRequest>
              output: <tpf.embedding.EmbeddingResult>
            steps:
              - name: Embed boundary
                service: com.example.transport.EmbeddingBoundaryService
                cardinality: ONE_TO_ONE
                input: <tpf.embedding.EmbeddingRequest>
                output: <tpf.embedding.EmbeddingResult>
                java:
                  input: org.pipelineframework.connector.embedding.EmbeddingRequest
                  output: org.pipelineframework.connector.embedding.EmbeddingResult
            """, embeddingService());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        assertGenerated(fixture, "com/example/transport/dto/EmbeddingRequestDto.java",
            "public record EmbeddingRequestDto(String itemId, String text)");
        assertGenerated(fixture, "com/example/transport/dto/EmbeddingResultDto.java",
            "java.util.List<Float> values");
        String requestMapper = generatedContaining(fixture.generated(), "RestMapper.java", "EmbeddingRequest");
        assertThat(requestMapper).contains("implements org.pipelineframework.mapper.Mapper<org.pipelineframework.connector.embedding.EmbeddingRequest");
        assertThat(requestMapper).contains("new org.pipelineframework.connector.embedding.EmbeddingRequest(");
        assertThat(requestMapper).doesNotContain("Json");

        Object roundTripped = roundTrip(
            fixture,
            "EmbeddingResult",
            EmbeddingResult.class,
            new EmbeddingResult("item", "text", List.of(0.25f, -0.5f, 0.25f)));
        assertThat(roundTripped).isEqualTo(new EmbeddingResult("item", "text", List.of(0.25f, -0.5f, 0.25f)));
    }

    @Test
    void restRecursivelyGeneratesNestedRepeatedContributedRecords() throws Exception {
        Fixture fixture = compile("REST", "COMPUTE", """
            contract:
              input: <tpf.vector.VectorSearchRequest>
              output: <tpf.vector.VectorSearchResult>
            steps:
              - name: Search boundary
                service: com.example.transport.VectorBoundaryService
                cardinality: ONE_TO_ONE
                input: <tpf.vector.VectorSearchRequest>
                output: <tpf.vector.VectorSearchResult>
                java:
                  input: org.pipelineframework.connector.vector.VectorSearchRequest
                  output: org.pipelineframework.connector.vector.VectorSearchResult
            """, vectorService());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        assertGenerated(fixture, "com/example/transport/dto/VectorMatchDto.java", "Float score");
        assertGenerated(fixture, "com/example/transport/dto/VectorSearchResultDto.java",
            "java.util.List<com.example.transport.dto.VectorMatchDto> matches");
        String mapper = generatedContaining(fixture.generated(), "RestMapper.java", "VectorSearchResult");
        assertThat(mapper).contains(".stream().map(matchesMapper::fromExternal).toList()");
        assertThat(mapper).contains(".stream().map(matchesMapper::toExternal).toList()");

        VectorSearchResult result = new VectorSearchResult(
            "query", "text", List.of(new VectorMatch("a", "alpha", 0.75f), new VectorMatch("b", "beta", 0.5f)));
        assertThat(roundTrip(fixture, "VectorSearchResult", VectorSearchResult.class, result)).isEqualTo(result);
    }

    @Test
    void functionReusesTheCompilerOwnedRestRepresentation() throws IOException {
        Fixture fixture = compile("REST", "FUNCTION", """
            contract:
              input: <tpf.embedding.EmbeddingRequest>
              output: <tpf.embedding.EmbeddingResult>
            steps:
              - name: Embed boundary
                service: com.example.transport.EmbeddingBoundaryService
                cardinality: ONE_TO_ONE
                input: <tpf.embedding.EmbeddingRequest>
                output: <tpf.embedding.EmbeddingResult>
                java:
                  input: org.pipelineframework.connector.embedding.EmbeddingRequest
                  output: org.pipelineframework.connector.embedding.EmbeddingResult
            """, embeddingService());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        assertGenerated(fixture, "com/example/transport/dto/EmbeddingRequestDto.java", "record EmbeddingRequestDto");
        String handler = generatedContaining(fixture.generated(), "FunctionHandler.java", "EmbeddingRequestDto");
        assertThat(handler).contains("com.example.transport.dto.EmbeddingRequestDto");
    }

    @Test
    void grpcGeneratesTypedProtobufMappersForContributedRecords() throws IOException {
        Fixture fixture = compile("GRPC", "COMPUTE", """
            contract:
              input: <tpf.embedding.EmbeddingRequest>
              output: <tpf.embedding.EmbeddingResult>
            steps:
              - name: Embed boundary
                service: com.example.transport.EmbeddingBoundaryService
                cardinality: ONE_TO_ONE
                input: <tpf.embedding.EmbeddingRequest>
                output: <tpf.embedding.EmbeddingResult>
                java:
                  input: org.pipelineframework.connector.embedding.EmbeddingRequest
                  output: org.pipelineframework.connector.embedding.EmbeddingResult
            """, embeddingService());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        String mapper = generatedContaining(fixture.generated(), "GrpcMapper.java", "EmbeddingResult");
        assertThat(mapper).contains("org.pipelineframework.connector.embedding.EmbeddingResult fromProto");
        assertThat(mapper).contains("PipelineTypes.EmbeddingResult toProto");
        assertThat(mapper).contains("value.getValuesList()");
        assertThat(mapper).doesNotContain("Json");
    }

    @Test
    void grpcGeneratesNestedRepeatedContributedRecordMappings() throws IOException {
        Fixture fixture = compile("GRPC", "COMPUTE", """
            contract:
              input: <tpf.vector.VectorSearchRequest>
              output: <tpf.vector.VectorSearchResult>
            steps:
              - name: Search boundary
                service: com.example.transport.VectorBoundaryService
                cardinality: ONE_TO_ONE
                input: <tpf.vector.VectorSearchRequest>
                output: <tpf.vector.VectorSearchResult>
                java:
                  input: org.pipelineframework.connector.vector.VectorSearchRequest
                  output: org.pipelineframework.connector.vector.VectorSearchResult
            """, vectorService());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        String mapper = generatedContaining(fixture.generated(), "GrpcMapper.java", "VectorSearchResult");
        assertThat(mapper).contains("value.getMatchesList().stream().map(");
        assertThat(mapper).contains("VectorMatch_");
        assertThat(mapper).contains("::fromProto).toList()");
        assertThat(mapper).contains("domain.matches().stream().map(");
        assertThat(mapper).contains("::toProto).toList()");
    }

    @Test
    void queryClientMapsRestInputToTheConnectorRecordAndResultBack() throws IOException {
        Fixture fixture = compile("REST", "COMPUTE", """
            contract:
              input: <tpf.embedding.EmbeddingRequest>
              output: <tpf.embedding.EmbeddingResult>
            connectors:
              embedder: { provider: proof.embedding, version: 1, config: {} }
            steps:
              - name: Embed
                kind: query
                cardinality: ONE_TO_ONE
                input: <tpf.embedding.EmbeddingRequest>
                output: <tpf.embedding.EmbeddingResult>
                using: embedder
                operation: embed
                operationVersion: 1
                capture: { keyFields: [itemId, text] }
                java:
                  input: org.pipelineframework.connector.embedding.EmbeddingRequest
                  output: org.pipelineframework.connector.embedding.EmbeddingResult
            """, markerSource());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        String client = generatedContaining(fixture.generated(), "QueryClientStep.java", "EmbeddingRequest queryInput");
        assertThat(client).contains("EmbeddingRequest queryInput = inputMapper.fromExternal(input)");
        assertThat(client).contains("support.queryOneToOne(");
        assertThat(client).contains("EmbeddingResult.class).map(outputMapper::toExternal)");
        String contract = fixture.compilation()
            .generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", "pipeline-contract.json")
            .orElseThrow()
            .getCharContent(true)
            .toString();
        assertThat(contract).contains("tpf.embedding.EmbeddingRequest");
        assertThat(contract).contains("tpf.embedding.EmbeddingResult");
        assertThat(contract).doesNotContain("RestMapper");
        assertThat(contract).doesNotContain("EmbeddingRequestDto");
    }

    @Test
    void queryClientMapsGrpcMessagesAroundTheConnectorRecord() throws IOException {
        Fixture fixture = compile("GRPC", "COMPUTE", """
            contract:
              input: <tpf.embedding.EmbeddingRequest>
              output: <tpf.embedding.EmbeddingResult>
            connectors:
              embedder: { provider: proof.embedding, version: 1, config: {} }
            steps:
              - name: Embed
                kind: query
                cardinality: ONE_TO_ONE
                input: <tpf.embedding.EmbeddingRequest>
                output: <tpf.embedding.EmbeddingResult>
                using: embedder
                operation: embed
                operationVersion: 1
                capture: { keyFields: [itemId, text] }
                java:
                  input: org.pipelineframework.connector.embedding.EmbeddingRequest
                  output: org.pipelineframework.connector.embedding.EmbeddingResult
            """, markerSource());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        String client = generatedContaining(fixture.generated(), "QueryClientStep.java", "EmbeddingRequest queryInput");
        assertThat(client).contains("PipelineTypes.EmbeddingRequest input");
        assertThat(client).contains("EmbeddingRequest queryInput = inputMapper.fromGrpc(input)");
        assertThat(client).contains("EmbeddingResult.class).map(outputMapper::toGrpc)");
    }

    @Test
    void commandClientRetainsCommandSupportAroundTypedTransportMapping() throws IOException {
        Fixture fixture = compile("REST", "COMPUTE", """
            contract:
              input: <tpf.vector.VectorUpsertRequest>
              output: <tpf.vector.VectorUpsertResult>
            connectors:
              vectors: { provider: proof.vector, version: 1, config: {} }
            steps:
              - name: Upsert
                kind: command
                cardinality: ONE_TO_ONE
                input: <tpf.vector.VectorUpsertRequest>
                output: <tpf.vector.VectorUpsertResult>
                using: vectors
                operation: upsert
                operationVersion: 1
                commandIdGenerator: com.example.transport.UpsertCommandId
                duplicatePolicy: RETURN_RECORDED
                config: {}
                java:
                  input: org.pipelineframework.connector.vector.VectorUpsertRequest
                  output: org.pipelineframework.connector.vector.VectorUpsertResult
            """, commandIdSource());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        String client = generatedContaining(fixture.generated(), "CommandClientStep.java", "VectorUpsertRequest commandInput");
        assertThat(client).contains("VectorUpsertRequest commandInput = inputMapper.fromExternal(input)");
        assertThat(client).contains("CommandStepSupport support");
        assertThat(client).contains("support.<VectorUpsertRequest, VectorUpsertResult>execute(");
        assertThat(client).contains(".map(commandOutput -> outputMapper.toExternal(commandOutput))");
    }

    @Test
    void commandClientMapsGrpcMessagesWithoutChangingEffectSupport() throws IOException {
        Fixture fixture = compile("GRPC", "COMPUTE", """
            contract:
              input: <tpf.vector.VectorUpsertRequest>
              output: <tpf.vector.VectorUpsertResult>
            connectors:
              vectors: { provider: proof.vector, version: 1, config: {} }
            steps:
              - name: Upsert
                kind: command
                cardinality: ONE_TO_ONE
                input: <tpf.vector.VectorUpsertRequest>
                output: <tpf.vector.VectorUpsertResult>
                using: vectors
                operation: upsert
                operationVersion: 1
                commandIdGenerator: com.example.transport.UpsertCommandId
                duplicatePolicy: RETURN_RECORDED
                config: {}
                java:
                  input: org.pipelineframework.connector.vector.VectorUpsertRequest
                  output: org.pipelineframework.connector.vector.VectorUpsertResult
            """, commandIdSource());

        assertEquals(Compilation.Status.SUCCESS, fixture.compilation().status(), fixture.compilation().diagnostics().toString());
        String client = generatedContaining(fixture.generated(), "CommandClientStep.java", "VectorUpsertRequest commandInput");
        assertThat(client).contains("PipelineTypes.VectorUpsertRequest input");
        assertThat(client).contains("VectorUpsertRequest commandInput = inputMapper.fromGrpc(input)");
        assertThat(client).contains("CommandStepSupport support");
        assertThat(client).contains(".map(commandOutput -> outputMapper.toGrpc(commandOutput))");
    }

    private Fixture compile(String transport, String platform, String body, javax.tools.JavaFileObject service)
        throws IOException {
        Path generated = tempDir.resolve(transport.toLowerCase() + "-" + platform.toLowerCase())
            .resolve("target/generated-sources/pipeline");
        Files.createDirectories(generated);
        Path config = tempDir.resolve("pipeline-" + transport.toLowerCase() + "-" + platform.toLowerCase() + ".yaml");
        Files.writeString(config, """
            version: 3
            appName: Contributed transport
            basePackage: com.example.transport
            transport: %s
            platform: %s
            types:
              Marker: { fields: [[value, string]] }
            %s
            """.formatted(transport, platform, body));
        List<String> options = new java.util.ArrayList<>(List.of(
            "-Apipeline.config=" + config.toString().replace('\\', '/'),
            "-Apipeline.generatedSourcesDir=" + generated.toString().replace('\\', '/'),
            "-Apipeline.transport=" + transport,
            "-Apipeline.platform=" + platform));
        if ("GRPC".equals(transport)) {
            Path descriptor = tempDir.resolve("contributed-transport.dsc");
            writeDescriptor(descriptor);
            options.add("-Aprotobuf.descriptor.file=" + descriptor.toString().replace('\\', '/'));
        }
        Path metadataRoot = tempDir.resolve("connector-metadata");
        writeConnectorMetadata(metadataRoot);
        Compilation compilation;
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {metadataRoot.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            compilation = Compiler.javac()
                .withProcessors(new PipelineStepProcessor())
                .withOptions(options)
                .compile(service);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        return new Fixture(generated, compilation);
    }

    private static javax.tools.JavaFileObject embeddingService() {
        return JavaFileObjects.forSourceString("com.example.transport.EmbeddingBoundaryService", """
            package com.example.transport;
            public class EmbeddingBoundaryService implements org.pipelineframework.service.ReactiveService<
                    org.pipelineframework.connector.embedding.EmbeddingRequest,
                    org.pipelineframework.connector.embedding.EmbeddingResult> {
                public io.smallrye.mutiny.Uni<org.pipelineframework.connector.embedding.EmbeddingResult> process(
                        org.pipelineframework.connector.embedding.EmbeddingRequest input) {
                    return io.smallrye.mutiny.Uni.createFrom().item(
                        new org.pipelineframework.connector.embedding.EmbeddingResult(
                            input.itemId(), input.text(), java.util.List.of(1.0f)));
                }
            }
            """);
    }

    private static javax.tools.JavaFileObject vectorService() {
        return JavaFileObjects.forSourceString("com.example.transport.VectorBoundaryService", """
            package com.example.transport;
            public class VectorBoundaryService implements org.pipelineframework.service.ReactiveService<
                    org.pipelineframework.connector.vector.VectorSearchRequest,
                    org.pipelineframework.connector.vector.VectorSearchResult> {
                public io.smallrye.mutiny.Uni<org.pipelineframework.connector.vector.VectorSearchResult> process(
                        org.pipelineframework.connector.vector.VectorSearchRequest input) {
                    return io.smallrye.mutiny.Uni.createFrom().item(
                        new org.pipelineframework.connector.vector.VectorSearchResult(
                            input.queryId(), input.queryText(), java.util.List.of()));
                }
            }
            """);
    }

    private static javax.tools.JavaFileObject markerSource() {
        return JavaFileObjects.forSourceString(
            "com.example.transport.MarkerSource",
            "package com.example.transport; public final class MarkerSource { }");
    }

    private static javax.tools.JavaFileObject commandIdSource() {
        return JavaFileObjects.forSourceString("com.example.transport.UpsertCommandId", """
            package com.example.transport;
            public final class UpsertCommandId implements org.pipelineframework.command.CommandIdGenerator<
                    org.pipelineframework.connector.vector.VectorUpsertRequest> {
                public String commandId(
                        org.pipelineframework.command.CommandDescriptor descriptor,
                        org.pipelineframework.connector.vector.VectorUpsertRequest input) {
                    return input.itemId();
                }
            }
            """);
    }

    private static void writeConnectorMetadata(Path root) throws IOException {
        Path manifest = root.resolve("META-INF/pipeline/connector-providers.json");
        Files.createDirectories(manifest.getParent());
        Files.writeString(manifest, """
            {"schemaVersion":1,"providers":[
              {"id":"proof.embedding","version":{"major":1,"minor":0},"operations":[
                {"id":"embed","kind":"tpf:query","majorVersion":1,
                 "queryCapabilities":{"cacheability":"CACHEABLE"}}]},
              {"id":"proof.vector","version":{"major":1,"minor":0},"operations":[
                {"id":"upsert","kind":"tpf:command","majorVersion":1,
                 "commandCapabilities":{"retryRedriveSupported":false,
                 "providerIdempotencySupported":true,"reconciliationSupported":false,
                 "executionPosture":"AUTOMATED","maximumMachineConfirmation":"PROVIDER_ACKNOWLEDGED",
                 "userConfirmationSupported":false,"durableReferenceKinds":[]}}]}
            ]}
            """);
    }

    private static void writeDescriptor(Path path) throws IOException {
        DescriptorProtos.DescriptorProto request = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("EmbeddingRequest")
            .addField(field("item_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("text", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .build();
        DescriptorProtos.DescriptorProto result = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("EmbeddingResult")
            .addField(field("item_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("text", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("values", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, true))
            .build();
        DescriptorProtos.DescriptorProto upsertRequest = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("VectorUpsertRequest")
            .addField(field("item_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("content", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("values", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, true))
            .build();
        DescriptorProtos.DescriptorProto upsertResult = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("VectorUpsertResult")
            .addField(field("item_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .build();
        DescriptorProtos.DescriptorProto searchRequest = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("VectorSearchRequest")
            .addField(field("query_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("query_text", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("values", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, true))
            .addField(field("limit", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, false))
            .build();
        DescriptorProtos.DescriptorProto match = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("VectorMatch")
            .addField(field("item_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("content", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("score", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, false))
            .build();
        DescriptorProtos.DescriptorProto searchResult = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("VectorSearchResult")
            .addField(field("query_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(field("query_text", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, false))
            .addField(messageField("matches", 3, ".com.example.transport.grpc.VectorMatch", true))
            .build();
        DescriptorProtos.ServiceDescriptorProto service = DescriptorProtos.ServiceDescriptorProto.newBuilder()
            .setName("ProcessEmbedBoundaryService")
            .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                .setName("remoteProcess")
                .setInputType(".com.example.transport.grpc.EmbeddingRequest")
                .setOutputType(".com.example.transport.grpc.EmbeddingResult"))
            .build();
        DescriptorProtos.ServiceDescriptorProto vectorService = DescriptorProtos.ServiceDescriptorProto.newBuilder()
            .setName("ProcessSearchBoundaryService")
            .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                .setName("remoteProcess")
                .setInputType(".com.example.transport.grpc.VectorSearchRequest")
                .setOutputType(".com.example.transport.grpc.VectorSearchResult"))
            .build();
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("pipeline.proto")
            .setPackage("com.example.transport.grpc")
            .setSyntax("proto3")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.transport.grpc")
                .setJavaOuterClassname("PipelineTypes"))
            .addMessageType(request)
            .addMessageType(result)
            .addMessageType(upsertRequest)
            .addMessageType(upsertResult)
            .addMessageType(searchRequest)
            .addMessageType(match)
            .addMessageType(searchResult)
            .addService(service)
            .addService(vectorService)
            .build();
        Files.write(path, DescriptorProtos.FileDescriptorSet.newBuilder().addFile(file).build().toByteArray());
    }

    private static DescriptorProtos.FieldDescriptorProto field(
        String name,
        int number,
        DescriptorProtos.FieldDescriptorProto.Type type,
        boolean repeated
    ) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName(name)
            .setNumber(number)
            .setType(type)
            .setLabel(repeated
                ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();
    }

    private static DescriptorProtos.FieldDescriptorProto messageField(
        String name,
        int number,
        String typeName,
        boolean repeated
    ) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName(name)
            .setNumber(number)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(typeName)
            .setLabel(repeated
                ? DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
                : DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();
    }

    private static void assertGenerated(Fixture fixture, String relative, String expected) throws IOException {
        Path source;
        try (var files = Files.walk(fixture.generated())) {
            source = files.filter(Files::isRegularFile)
                .filter(path -> path.toString().replace('\\', '/').endsWith(relative))
                .findFirst()
                .orElse(null);
        }
        assertTrue(source != null, "Missing generated source " + relative + "; generated: "
            + generatedFiles(fixture.generated()));
        assertThat(Files.readString(source)).contains(expected);
    }

    private static List<String> generatedFiles(Path generated) throws IOException {
        try (var files = Files.walk(generated)) {
            return files.filter(Files::isRegularFile).map(generated::relativize).map(Path::toString).sorted().toList();
        }
    }

    private Object roundTrip(Fixture fixture, String marker, Class<?> domainType, Object value) throws Exception {
        List<Path> transportSources;
        try (var files = Files.walk(fixture.generated())) {
            transportSources = files.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> {
                    String normalized = path.toString().replace('\\', '/');
                    return normalized.contains("/dto/") || normalized.contains("/transport/generated/");
                })
                .toList();
        }
        Path classes = tempDir.resolve("compiled-" + marker);
        Files.createDirectories(classes);
        var compiler = ToolProvider.getSystemJavaCompiler();
        try (var manager = compiler.getStandardFileManager(null, null, null)) {
            var units = manager.getJavaFileObjectsFromPaths(transportSources);
            boolean compiled = compiler.getTask(
                null,
                manager,
                null,
                List.of("-classpath", System.getProperty("java.class.path"), "-d", classes.toString()),
                null,
                units).call();
            assertTrue(compiled, "Generated transport sources must compile");
        }
        Path mapperSource;
        try (var files = Files.walk(fixture.generated())) {
            mapperSource = files.filter(path -> path.getFileName().toString().endsWith("RestMapper.java"))
                .filter(path -> {
                    try {
                        return Files.readString(path).contains(marker);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .findFirst()
                .orElseThrow();
        }
        String mapperSourceText = Files.readString(mapperSource);
        String packageName = mapperSourceText.substring("package ".length(), mapperSourceText.indexOf(';'));
        String className = mapperSource.getFileName().toString().replace(".java", "");
        try (URLClassLoader loader = new URLClassLoader(
                new java.net.URL[] {classes.toUri().toURL()}, getClass().getClassLoader())) {
            Object mapper = loader.loadClass(packageName + "." + className).getConstructor().newInstance();
            Object dto = mapper.getClass().getMethod("toExternal", domainType).invoke(mapper, value);
            return mapper.getClass().getMethod("fromExternal", dto.getClass()).invoke(mapper, dto);
        }
    }

    private static String generatedContaining(Path generated, String suffix, String marker) throws IOException {
        try (var files = Files.walk(generated)) {
            List<Path> candidates = files.filter(path -> path.getFileName().toString().endsWith(suffix)).toList();
            for (Path candidate : candidates) {
                String source = Files.readString(candidate);
                if (source.contains(marker)) {
                    return source;
                }
            }
        }
        throw new AssertionError("No generated " + suffix + " contains " + marker);
    }

    private record Fixture(Path generated, Compilation compilation) {
    }
}
