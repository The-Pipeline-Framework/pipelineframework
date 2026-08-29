package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

class QueryClientStepRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void targetReturnsQueryClientStepTarget() {
        assertEquals(GenerationTarget.QUERY_CLIENT_STEP, new QueryClientStepRenderer().target());
    }

    @Test
    void rendersOneShotDynamicOperationAdapter() throws IOException {
        PipelineStepModel model = model(
            ClassName.get("com.example.common.domain", "AgentCall"),
            ClassName.get("com.example.common.domain", "OperationObservation"));

        new QueryClientStepRenderer().renderDynamicOperation(model, generationContext("LOCAL"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskDynamicOperationClientStep.java"));
        assertTrue(source.contains("OperationDispatchSupport support"));
        assertTrue(source.contains("OperationDispatchDescriptorFactory descriptorFactory"));
        assertTrue(source.contains("support.dispatch(descriptorFactory.descriptor(\"LoadCustomerRisk\"), "
            + "input.binding(), input.operation(), input.argumentsJson(), OperationObservation.class)"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"LOCAL", "REST", "GRPC"})
    void rendersReactiveStepThatDelegatesToQuerySupport(String transport) throws IOException {
        PipelineStepModel model = model(
            ClassName.get("com.example.common.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.common.domain", "CustomerRiskSnapshot"));

        new QueryClientStepRenderer().render(model, generationContext(transport));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));

        switch (transport) {
            case "LOCAL" -> {
                assertTrue(source.contains("implements StepOneToOne<CustomerRiskLookup, CustomerRiskSnapshot>"));
                assertTrue(source.contains("\"com.example.common.domain.CustomerRiskLookup\", "
                    + "\"com.example.common.domain.CustomerRiskSnapshot\""));
            }
            case "REST" -> {
                assertTrue(source.contains("implements StepOneToOne<CustomerRiskLookupDto, CustomerRiskSnapshotDto>"));
                assertTrue(source.contains("\"com.example.common.dto.CustomerRiskLookupDto\", "
                    + "\"com.example.common.dto.CustomerRiskSnapshotDto\""));
            }
            case "GRPC" -> {
                assertTrue(source.contains(
                    "implements StepOneToOne<PipelineTypes.CustomerRiskLookup, PipelineTypes.CustomerRiskSnapshot>"));
                assertTrue(source.contains("\"com.example.grpc.PipelineTypes.CustomerRiskLookup\", "
                    + "\"com.example.grpc.PipelineTypes.CustomerRiskSnapshot\""));
            }
            default -> throw new IllegalArgumentException("Unexpected transport " + transport);
        }
        assertTrue(source.contains("QueryStepSupport support"));
        assertTrue(source.contains("QueryStepDescriptorFactory descriptorFactory"));
        assertTrue(source.contains("support.queryOneToOne(descriptorFactory.descriptor(\"LoadCustomerRisk\", "));
    }

    @Test
    void rendersStreamingQueryIntoTheExistingOneToManyStepContractWithoutGenericCache() throws IOException {
        PipelineStepModel model = model(
            ClassName.get("com.example.common.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.common.domain", "CustomerRiskSnapshot"),
            StreamingShape.UNARY_STREAMING);

        new QueryClientStepRenderer().render(model, generationContext("LOCAL"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));
        assertTrue(source.contains("implements StepOneToMany<CustomerRiskLookup, CustomerRiskSnapshot>"));
        assertTrue(source.contains("Multi<CustomerRiskSnapshot> applyOneToMany(CustomerRiskLookup input)"));
        assertTrue(source.contains("support.queryOneToMany(descriptorFactory.descriptor(\"LoadCustomerRisk\", "));
        assertTrue(!source.contains("CacheKeyTarget"));
        assertTrue(!source.contains("ProviderQueryStep"));
    }

    @Test
    void fallsBackToConfiguredBasePackageForNonStandardDomainPackage() throws IOException {
        PipelineStepModel model = model(
            ClassName.get("com.example.risk.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.risk.domain", "CustomerRiskSnapshot"));

        new QueryClientStepRenderer().render(model, generationContext(PipelineTransport.REST, "com.example"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));

        assertTrue(source.contains("import com.example.common.dto.CustomerRiskLookupDto;"));
        assertTrue(source.contains("import com.example.common.dto.CustomerRiskSnapshotDto;"));
    }

    @Test
    void usesConfiguredBasePackageForBlankDomainPackage() throws IOException {
        PipelineStepModel model = model(
            ClassName.get("", "CustomerRiskLookup"),
            ClassName.get("", "CustomerRiskSnapshot"));

        new QueryClientStepRenderer().render(model, generationContext(PipelineTransport.GRPC, "com.example"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));

        assertTrue(source.contains("import com.example.grpc.PipelineTypes;"));
        assertTrue(source.contains(
            "implements StepOneToOne<PipelineTypes.CustomerRiskLookup, PipelineTypes.CustomerRiskSnapshot>"));
    }

    @Test
    void rejectsUnrecognizedDomainPackageWithoutConfiguredBasePackage() {
        PipelineStepModel model = model(
            ClassName.get("com.example.risk.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.risk.domain", "CustomerRiskSnapshot"));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            new QueryClientStepRenderer().render(model, generationContext(PipelineTransport.REST, null)));

        assertTrue(exception.getMessage().contains("does not match .common.domain, .common.dto, or .service"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Load Customer Risk", "Load-Customer_Risk", "Load\\tCustomer Risk"})
    void generatedLocalNativeQueryCarriesStaticCacheRequirementsAcrossNameSeparators(String stepName) throws Exception {
        Path metadataRoot = tempDir.resolve("connector-metadata");
        Path manifest = metadataRoot.resolve("META-INF/pipeline/connector-providers.json");
        Files.createDirectories(manifest.getParent());
        Files.writeString(manifest, """
            {"schemaVersion":1,"providers":[{"id":"acme.lookup","version":{"major":1,"minor":0},
            "operations":[{"id":"customer.find","kind":"tpf:query","majorVersion":1,
            "queryCapabilities":{"cacheability":"CACHEABLE","maximumCacheAge":"PT5M",
            "maximumNegativeCacheTtl":"PT30S"}}]}]}
            """);
        Path pipeline = tempDir.resolve("pipeline.yaml");
        Files.writeString(pipeline, """
            basePackage: com.example
            transport: LOCAL
            connectors:
              lookup:
                provider: acme.lookup
                version: 1
            steps:
              - name: "%s"
                kind: query
                operation: customer.find
                using: lookup
                negativeCacheTtl: PT20S
            """.formatted(stepName));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[] { metadataRoot.toUri().toURL() }, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            PipelineStepModel model = model(
                ClassName.get("com.example.common.domain", "CustomerRiskLookup"),
                ClassName.get("com.example.common.domain", "CustomerRiskSnapshot"));

            new QueryClientStepRenderer().render(model, generationContext(Map.of(
                "pipeline.config", pipeline.toString(),
                "pipeline.transport", "LOCAL")));

            String source = Files.readString(tempDir.resolve(
                "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));
            assertTrue(source.contains("ProviderQueryStep"));
            assertTrue(source.contains("QueryCacheRequirements queryCacheRequirements()"));
            assertTrue(source.contains("ConnectorProviderId.of(\"acme.lookup\")"));
            assertTrue(source.contains("\"customer.find\""));
            assertTrue(source.contains("Duration.parse(\"PT5M\")"));
            assertTrue(source.contains("Duration.parse(\"PT20S\")"));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void generatedJpaQueryUsesTheOutputPersistenceRepresentationMapper() throws Exception {
        Path metadataRoot = tempDir.resolve("jpa-metadata");
        Path manifest = metadataRoot.resolve("META-INF/pipeline/connector-providers.json");
        Files.createDirectories(manifest.getParent());
        Files.writeString(manifest, """
            {"schemaVersion":1,"providers":[{"id":"jpa.query","version":{"major":1,"minor":0},
            "operations":[{"id":"find.one","kind":"tpf:query","majorVersion":1,
            "queryCapabilities":{"cacheability":"CACHEABLE"}}]}]}
            """);
        Path pipeline = tempDir.resolve("mapped-jpa-query.yaml");
        Files.writeString(pipeline, """
            version: 3
            appName: Mapped JPA Query
            basePackage: com.example
            transport: LOCAL
            types:
              RedriveAnalysis: { fields: [[documentId, uuid]] }
              InvoiceFiles:
                fields: [[documentId, uuid]]
                mappings:
                  persistence:
                    type: com.example.persistence.InvoiceFilesEntity
                    mapper: com.example.persistence.InvoiceFilesPersistenceMapper
            connectors:
              jpa:
                provider: jpa.query
                version: 1
            steps:
              - name: Load Customer Risk
                kind: query
                cardinality: ONE_TO_ONE
                input: RedriveAnalysis
                output: InvoiceFiles
                operation: find.one
                using: jpa
                config:
                  entity: com.example.persistence.InvoiceFilesEntity
                  where:
                    documentId: { operator: eq, values: [input.documentId] }
                  result: single
            """);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[] { metadataRoot.toUri().toURL() }, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            PipelineStepModel model = model(
                ClassName.get("com.example.domain", "RedriveAnalysis"),
                ClassName.get("com.example.domain", "InvoiceFiles"));

            new QueryClientStepRenderer().render(model, generationContext(Map.of(
                "pipeline.config", pipeline.toString(),
                "pipeline.transport", "LOCAL")));

            String source = Files.readString(tempDir.resolve(
                "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));
            assertTrue(source.contains("InvoiceFilesPersistenceMapper representationMapper"));
            assertTrue(source.contains(
                "InvoiceFiles.class, InvoiceFilesEntity.class, representationMapper"));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private PipelineStepModel model(ClassName inputType, ClassName outputType) {
        return model(inputType, outputType, StreamingShape.UNARY_UNARY);
    }

    private PipelineStepModel model(
        ClassName inputType,
        ClassName outputType,
        StreamingShape streamingShape
    ) {
        return new PipelineStepModel.Builder()
            .serviceName("LoadCustomerRisk")
            .generatedName("LoadCustomerRiskService")
            .servicePackage("com.example.risk")
            .serviceClassName(ClassName.get("org.pipelineframework.query", "QueryStepDescriptor"))
            .streamingShape(streamingShape)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(inputType, null, false))
            .outputMapping(new TypeMapping(outputType, null, false))
            .enabledTargets(Set.of(GenerationTarget.QUERY_CLIENT_STEP))
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();
    }

    private GenerationContext generationContext(String transport) {
        return generationContext(Map.of("pipeline.transport", transport));
    }

    private GenerationContext generationContext(Map<String, String> options) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(options);
        return new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null);
    }

    private GenerationContext generationContext(PipelineTransport transport, String basePackage) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        return new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null,
            transport,
            basePackage);
    }
}
