package org.pipelineframework.processor;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConnectorBootstrapGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesBootstrapForDeclaredLiveConnector() throws IOException {
        Path generatedSourcesDir = tempDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);
        Files.writeString(tempDir.resolve("pipeline.yaml"), """
            appName: "Connector Test"
            basePackage: "com.example.pipeline"
            transport: "LOCAL"
            steps:
              - name: "Order Ready"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "OrderRequest"
                outputTypeName: "ReadyOrder"
            connectors:
              - name: "orders-to-delivery"
                transport: "GRPC"
                mapper: "com.example.connector.ReadyOrderConnectorMapper"
                idempotency: "PRE_FORWARD"
                backpressure: "BUFFER"
                failureMode: "PROPAGATE"
                source:
                  kind: "OUTPUT_BUS"
                  step: "Order Ready"
                  type: "com.example.connector.ReadyOrderMessage"
                target:
                  kind: "LIVE_INGEST"
                  pipeline: "deliver-order"
                  type: "com.example.connector.DispatchReadyOrderMessage"
                  adapter: "com.example.connector.DispatchConnectorTarget"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Apipeline.config=" + tempDir.resolve("pipeline.yaml"))
            .compile(
                pipelineStepStub(),
                domainStub("OrderRequest"),
                domainStub("ReadyOrder"),
                dtoStub("OrderRequestDto"),
                dtoStub("ReadyOrderDto"),
                mapperStub("OrderRequestMapper", "OrderRequest", "OrderRequestDto"),
                mapperStub("ReadyOrderMapper", "ReadyOrder", "ReadyOrderDto"),
                connectorSourceStub(),
                connectorDispatchTypeStub(),
                connectorTargetStub(),
                connectorMapperStub());

        assertThat(compilation).succeeded();

        JavaFileObject generatedSource = compilation.generatedFile(
            StandardLocation.SOURCE_OUTPUT,
            "com.example.pipeline.connector",
            "OrdersToDeliveryConnectorBridge.java").orElseThrow();
        String source = generatedSource.getCharContent(true).toString();
        assertTrue(source.contains("class OrdersToDeliveryConnectorBridge"),
            "Expected generated connector bridge class declaration");
        assertTrue(source.contains("new ConnectorRuntime<ReadyOrderMessage, DispatchReadyOrderMessage>"),
            "Expected generated connector runtime initialization");
        assertTrue(source.contains("new OutputBusConnectorSource<>(outputBus, ReadyOrderMessage.class)"),
            "Expected generated output bus source wiring");
        assertTrue(source.contains("targetAdapter"),
            "Expected generated connector target adapter wiring");
        assertTrue(source.contains("mapper.map(sourceRecord.payload())"),
            "Expected generated mapper invocation in connector bridge");
    }

    @Test
    void failsWhenMapperSignatureDoesNotMatchConnectorTypes() throws IOException {
        Path generatedSourcesDir = tempDir.resolve("bad-target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);
        Path configRoot = tempDir.resolve("bad-target");
        Files.createDirectories(configRoot);
        Files.writeString(configRoot.resolve("pipeline.yaml"), """
            appName: "Connector Test"
            basePackage: "com.example.pipeline"
            transport: "LOCAL"
            steps:
              - name: "Order Ready"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "OrderRequest"
                outputTypeName: "ReadyOrder"
            connectors:
              - name: "orders-to-delivery"
                transport: "GRPC"
                mapper: "com.example.connector.WrongConnectorMapper"
                source:
                  kind: "OUTPUT_BUS"
                  step: "Order Ready"
                  type: "com.example.connector.ReadyOrderMessage"
                target:
                  kind: "LIVE_INGEST"
                  pipeline: "deliver-order"
                  type: "com.example.connector.DispatchReadyOrderMessage"
                  adapter: "com.example.connector.DispatchConnectorTarget"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Apipeline.config=" + configRoot.resolve("pipeline.yaml"))
            .compile(
                pipelineStepStub(),
                domainStub("OrderRequest"),
                domainStub("ReadyOrder"),
                dtoStub("OrderRequestDto"),
                dtoStub("ReadyOrderDto"),
                mapperStub("OrderRequestMapper", "OrderRequest", "OrderRequestDto"),
                mapperStub("ReadyOrderMapper", "ReadyOrder", "ReadyOrderDto"),
                connectorSourceStub(),
                connectorDispatchTypeStub(),
                connectorTargetStub(),
                wrongConnectorMapperStub());

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(
            "must implement ConnectorMapper<com.example.connector.ReadyOrderMessage, com.example.connector.DispatchReadyOrderMessage>");
    }

    private JavaFileObject pipelineStepStub() {
        return JavaFileObjects.forSourceString(
            "com.example.pipeline.ProcessOrderReadyService",
            """
                package com.example.pipeline;

                import io.smallrye.mutiny.Uni;
                import jakarta.enterprise.context.ApplicationScoped;
                import org.pipelineframework.annotation.PipelineStep;
                import org.pipelineframework.service.ReactiveService;
                import org.pipelineframework.step.StepOneToOne;

                @PipelineStep(
                    inputType = com.example.domain.OrderRequest.class,
                    outputType = com.example.domain.ReadyOrder.class,
                    stepType = StepOneToOne.class,
                    inboundMapper = com.example.mapper.OrderRequestMapper.class,
                    outboundMapper = com.example.mapper.ReadyOrderMapper.class
                )
                @ApplicationScoped
                public class ProcessOrderReadyService
                    implements ReactiveService<com.example.domain.OrderRequest, com.example.domain.ReadyOrder> {

                    @Override
                    public Uni<com.example.domain.ReadyOrder> process(com.example.domain.OrderRequest input) {
                        return null;
                    }
                }
                """);
    }

    private JavaFileObject domainStub(String simpleName) {
        return JavaFileObjects.forSourceString(
            "com.example.domain." + simpleName,
            """
                package com.example.domain;

                public class %s {
                }
                """.formatted(simpleName));
    }

    private JavaFileObject dtoStub(String simpleName) {
        return JavaFileObjects.forSourceString(
            "com.example.dto." + simpleName,
            """
                package com.example.dto;

                public class %s {
                }
                """.formatted(simpleName));
    }

    private JavaFileObject mapperStub(String mapperName, String domainType, String dtoType) {
        return JavaFileObjects.forSourceString(
            "com.example.mapper." + mapperName,
            """
                package com.example.mapper;

                import jakarta.enterprise.context.ApplicationScoped;
                import org.pipelineframework.mapper.Mapper;

                @ApplicationScoped
                public class %s implements Mapper<com.example.domain.%s, com.example.dto.%s> {
                    @Override
                    public com.example.domain.%s fromExternal(com.example.dto.%s external) {
                        return null;
                    }

                    @Override
                    public com.example.dto.%s toExternal(com.example.domain.%s domain) {
                        return null;
                    }
                }
                """.formatted(mapperName, domainType, dtoType, domainType, dtoType, dtoType, domainType));
    }

    private JavaFileObject connectorSourceStub() {
        return JavaFileObjects.forSourceString(
            "com.example.connector.ReadyOrderMessage",
            """
                package com.example.connector;

                public class ReadyOrderMessage {
                }
                """);
    }

    private JavaFileObject connectorTargetStub() {
        return JavaFileObjects.forSourceString(
            "com.example.connector.DispatchConnectorTarget",
            """
                package com.example.connector;

                import io.smallrye.mutiny.Multi;
                import io.smallrye.mutiny.subscription.Cancellable;
                import jakarta.enterprise.context.ApplicationScoped;
                import org.pipelineframework.connector.ConnectorRecord;
                import org.pipelineframework.connector.ConnectorTarget;

                @ApplicationScoped
                public class DispatchConnectorTarget implements ConnectorTarget<DispatchReadyOrderMessage> {
                    @Override
                    public Cancellable forward(Multi<ConnectorRecord<DispatchReadyOrderMessage>> connectorStream) {
                        return connectorStream.subscribe().with(item -> {});
                    }
                }
                """);
    }

    private JavaFileObject connectorDispatchTypeStub() {
        return JavaFileObjects.forSourceString(
            "com.example.connector.DispatchReadyOrderMessage",
            """
                package com.example.connector;

                public class DispatchReadyOrderMessage {
                }
                """);
    }

    private JavaFileObject connectorMapperStub() {
        return JavaFileObjects.forSourceString(
            "com.example.connector.ReadyOrderConnectorMapper",
            """
                package com.example.connector;

                import jakarta.enterprise.context.ApplicationScoped;
                import org.pipelineframework.connector.ConnectorMapper;

                @ApplicationScoped
                public class ReadyOrderConnectorMapper
                    implements ConnectorMapper<ReadyOrderMessage, DispatchReadyOrderMessage> {

                    @Override
                    public DispatchReadyOrderMessage map(ReadyOrderMessage input) {
                        return new DispatchReadyOrderMessage();
                    }
                }
                """);
    }

    private JavaFileObject wrongConnectorMapperStub() {
        return JavaFileObjects.forSourceString(
            "com.example.connector.WrongConnectorMapper",
            """
                package com.example.connector;

                import jakarta.enterprise.context.ApplicationScoped;
                import org.pipelineframework.connector.ConnectorMapper;

                @ApplicationScoped
                public class WrongConnectorMapper
                    implements ConnectorMapper<DispatchReadyOrderMessage, ReadyOrderMessage> {

                    @Override
                    public ReadyOrderMessage map(DispatchReadyOrderMessage input) {
                        return new ReadyOrderMessage();
                    }
                }
                """);
    }
}
