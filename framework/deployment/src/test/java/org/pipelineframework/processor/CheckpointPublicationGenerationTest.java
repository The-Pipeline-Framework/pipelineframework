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

class CheckpointPublicationGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesCheckpointPublicationArtifacts() throws IOException {
        Path generatedSourcesDir = tempDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);
        Files.createDirectories(tempDir.resolve("src/main/resources"));
        Files.writeString(tempDir.resolve("src/main/resources/application.properties"), """
            pipeline.orchestrator.mode=QUEUE_ASYNC
            """);
        Files.writeString(tempDir.resolve("pipeline.yaml"), """
            appName: "Checkpoint Test"
            basePackage: "com.example.pipeline"
            transport: "LOCAL"
            input:
              subscription:
                publication: "orders-ready"
                mapper: "com.example.mapper.SubscriptionMapper"
            output:
              checkpoint:
                publication: "orders-dispatched"
                idempotencyKeyFields: ["orderId", "customerId"]
            steps:
              - name: "Order Ready"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.domain.OrderRequest"
                outputTypeName: "com.example.domain.ReadyOrder"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Apipeline.config=" + tempDir.resolve("pipeline.yaml"))
            .compile(
                orchestratorMarkerStub(),
                pipelineStepStub(),
                domainStub("OrderRequest"),
                domainStub("ReadyOrder"),
                dtoStub("OrderRequestDto"),
                dtoStub("ReadyOrderDto"),
                mapperStub("OrderRequestMapper", "OrderRequest", "OrderRequestDto"),
                mapperStub("ReadyOrderMapper", "ReadyOrder", "ReadyOrderDto"),
                mapperStub("SubscriptionMapper", "OrderRequest", "PublishedCheckpointDto"),
                extraDtoStub("PublishedCheckpointDto"));

        assertThat(compilation).succeeded();

        JavaFileObject publicationSource = compilation.generatedFile(
            StandardLocation.SOURCE_OUTPUT,
            "com.example.pipeline.orchestrator.service",
            "PipelineCheckpointPublicationDescriptor.java").orElseThrow();
        String publication = publicationSource.getCharContent(true).toString();
        assertTrue(publication.contains("class PipelineCheckpointPublicationDescriptor"));
        assertTrue(publication.contains("return \"orders-dispatched\";"));
        assertTrue(publication.contains("List.of(\"orderId\", \"customerId\")"));

        JavaFileObject subscriptionSource = compilation.generatedFile(
            StandardLocation.SOURCE_OUTPUT,
            "com.example.pipeline.orchestrator.service",
            "PipelineCheckpointSubscriptionHandler.java").orElseThrow();
        String subscription = subscriptionSource.getCharContent(true).toString();
        assertTrue(subscription.contains("class PipelineCheckpointSubscriptionHandler"));
        assertTrue(subscription.contains("return \"orders-ready\";"));
        assertTrue(subscription.contains("rawMapper.fromExternal(external)"));
        assertTrue(subscription.contains("pipelineExecutionService.executePipelineAsync(mapped, tenantId, idempotencyKey)"));

        JavaFileObject handoffMetadata = compilation.generatedFile(
            StandardLocation.CLASS_OUTPUT,
            "",
            "META-INF/pipeline/handoff.json").orElseThrow();
        String handoff = handoffMetadata.getCharContent(true).toString();
        assertTrue(handoff.contains("\"outputPublication\": \"orders-dispatched\""));
        assertTrue(handoff.contains("\"inputSubscription\": \"orders-ready\""));
        assertTrue(handoff.contains("\"runtimeIngressCapabilities\": ["));
        assertTrue(handoff.contains("\"HTTP_JSON\""));
        assertTrue(handoff.contains("\"HTTP_PROTO\""));
        assertTrue(handoff.contains("\"GRPC\""));
    }

    @Test
    void failsWhenLegacyConnectorsBlockIsDeclared() throws IOException {
        Path generatedSourcesDir = tempDir.resolve("bad/target/generated-sources/pipeline");
        Path configRoot = tempDir.resolve("bad");
        Files.createDirectories(generatedSourcesDir);
        Files.createDirectories(configRoot.resolve("src/main/resources"));
        Files.writeString(configRoot.resolve("src/main/resources/application.properties"), """
            pipeline.orchestrator.mode=QUEUE_ASYNC
            """);
        Files.writeString(configRoot.resolve("pipeline.yaml"), """
            appName: "Checkpoint Test"
            basePackage: "com.example.pipeline"
            transport: "LOCAL"
            connectors:
              - name: "orders-to-delivery"
                transport: "GRPC"
            steps:
              - name: "Order Ready"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "com.example.domain.OrderRequest"
                outputTypeName: "com.example.domain.ReadyOrder"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Apipeline.config=" + configRoot.resolve("pipeline.yaml"))
            .compile(
                orchestratorMarkerStub(),
                pipelineStepStub(),
                domainStub("OrderRequest"),
                domainStub("ReadyOrder"),
                dtoStub("OrderRequestDto"),
                dtoStub("ReadyOrderDto"),
                mapperStub("OrderRequestMapper", "OrderRequest", "OrderRequestDto"),
                mapperStub("ReadyOrderMapper", "ReadyOrder", "ReadyOrderDto"));

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(
            "Top-level connectors are no longer supported; use input.subscription and output.checkpoint");
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

    private JavaFileObject orchestratorMarkerStub() {
        return JavaFileObjects.forSourceString(
            "com.example.pipeline.OrchestratorMarker",
            """
                package com.example.pipeline;

                import org.pipelineframework.annotation.PipelineOrchestrator;

                @PipelineOrchestrator
                public class OrchestratorMarker {
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
            "com.example.pipeline.common.dto." + simpleName,
            """
                package com.example.pipeline.common.dto;

                public class %s {
                }
                """.formatted(simpleName));
    }

    private JavaFileObject extraDtoStub(String simpleName) {
        return JavaFileObjects.forSourceString(
            "com.example.external." + simpleName,
            """
                package com.example.external;

                public class %s {
                }
                """.formatted(simpleName));
    }

    private JavaFileObject mapperStub(String mapperName, String domainType, String dtoType) {
        String externalType = "PublishedCheckpointDto".equals(dtoType)
            ? "com.example.external." + dtoType
            : "com.example.pipeline.common.dto." + dtoType;
        return JavaFileObjects.forSourceString(
            "com.example.mapper." + mapperName,
            """
                package com.example.mapper;

                import jakarta.enterprise.context.ApplicationScoped;
                import org.pipelineframework.mapper.Mapper;

                @ApplicationScoped
                public class %s implements Mapper<com.example.domain.%s, %s> {
                    @Override
                    public com.example.domain.%s fromExternal(%s external) {
                        return null;
                    }

                    @Override
                    public %s toExternal(com.example.domain.%s domain) {
                        return null;
                    }
                }
                """.formatted(mapperName, domainType, externalType, domainType, externalType, externalType, domainType));
    }
}
