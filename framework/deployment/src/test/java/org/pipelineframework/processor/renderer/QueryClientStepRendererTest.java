package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ConnectorOperationSelection;
import org.pipelineframework.processor.ir.DynamicOperationSelection;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.processor.composition.PipelineReference;

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
            ClassName.get("com.example.common.domain", "OperationObservation"))
            .toBuilder().dynamicOperationSelection(dynamicSelection()).build();

        new QueryClientStepRenderer().renderDynamicOperation(model, generationContext("LOCAL"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskDynamicOperationClientStep.java"));
        assertTrue(source.contains("OperationDispatchSupport support"));
        assertTrue(source.contains("private static final OperationDispatchDescriptor descriptor"));
        assertTrue(source.contains("ConnectorBindingName.of(\"primary-lookup\")"));
        assertTrue(source.contains("ConnectorProviderId.of(\"proof.lookup\")"));
        assertTrue(source.contains("support.dispatch(descriptor, input.binding(), input.operation(), "
            + "input.argumentsJson(), input.contextJson(), OperationObservation.class)"));
        assertTrue(!source.contains("OperationDispatchDescriptorFactory"));
    }

    @Test
    void keepsIdenticallyNamedImportedDynamicStepsDistinctByQualifiedDefinition() throws IOException {
        PipelineStepModel first = model(
            ClassName.get("com.example.common.domain", "AgentCall"),
            ClassName.get("com.example.common.domain", "OperationObservation"))
            .toBuilder()
            .serviceName("DispatchBlock1111111111111111")
            .generatedName("DispatchBlock1111111111111111Service")
            .dynamicOperationSelection(dynamicSelection(
                "org.example.first/callable-loop", "org.example.first/callable-loop#Dispatch"))
            .build();
        PipelineStepModel second = model(
            ClassName.get("com.example.common.domain", "AgentCall"),
            ClassName.get("com.example.common.domain", "OperationObservation"))
            .toBuilder()
            .serviceName("DispatchBlock2222222222222222")
            .generatedName("DispatchBlock2222222222222222Service")
            .dynamicOperationSelection(dynamicSelection(
                "org.example.second/callable-loop", "org.example.second/callable-loop#Dispatch"))
            .build();

        QueryClientStepRenderer renderer = new QueryClientStepRenderer();
        renderer.renderDynamicOperation(first, generationContext("LOCAL"));
        renderer.renderDynamicOperation(second, generationContext("LOCAL"));

        String firstSource = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/DispatchBlock1111111111111111DynamicOperationClientStep.java"));
        String secondSource = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/DispatchBlock2222222222222222DynamicOperationClientStep.java"));
        assertTrue(firstSource.contains("org.example.first/callable-loop#Dispatch"));
        assertTrue(secondSource.contains("org.example.second/callable-loop#Dispatch"));
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
    void rendersOperationFirstQueryFromTypedSelectionWithoutReloadingApplicationYaml() throws IOException {
        ConnectorOperationSelection selection = ConnectorOperationSelection.query(
            "Execute operation",
            ConnectorBindingName.of("primary-graphql"),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("graphql.smallrye"), "execute.query",
                ConnectorOperationKind.QUERY, 1),
            1,
            Map.of("document", "catalogued"),
            new ConnectorOperationSelection.QuerySelection(
                QueryOperationCardinality.ONE_TO_ONE,
                QueryCapabilities.conservative(),
                Optional.empty(),
                Map.of(),
                java.util.List.of("operationKey")));
        PipelineStepModel model = model(
            ClassName.get("com.example.common.domain", "CustomerRiskLookup"),
            ClassName.get("com.example.common.domain", "CustomerRiskSnapshot"))
            .toBuilder().connectorOperationSelection(selection).build();

        new QueryClientStepRenderer().render(model, generationContext("LOCAL"));

        String source = Files.readString(tempDir.resolve(
            "com/example/risk/pipeline/LoadCustomerRiskQueryClientStep.java"));
        assertTrue(source.contains("QueryStepDescriptor.nativeQuery"));
        assertTrue(source.contains("ConnectorBindingName.of(\"primary-graphql\")"));
        assertTrue(source.contains("ConnectorProviderId.of(\"graphql.smallrye\")"));
        assertTrue(source.contains("\"execute.query\""));
        assertTrue(source.contains("support.queryOneToOne(QueryStepDescriptor.nativeQuery("));
        assertTrue(!source.contains("QueryStepDescriptorFactory"));
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

            new QueryClientStepRenderer().render(model, generationContextWithCompilerTypes(Map.of(
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

    private DynamicOperationSelection dynamicSelection() {
        return dynamicSelection(
            "org.pipelineframework.proof/callable-loop-proof",
            "org.pipelineframework.proof/callable-loop-proof#Dispatch");
    }

    private DynamicOperationSelection dynamicSelection(String definition, String runtimeStepId) {
        ConnectorOperationSelection operation = ConnectorOperationSelection.query(
            "Decide:lookup",
            ConnectorBindingName.of("primary-lookup"),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("proof.lookup"), "lookup", ConnectorOperationKind.QUERY, 1),
            1,
            Map.of("mode", "exact"),
            new ConnectorOperationSelection.QuerySelection(
                QueryOperationCardinality.ONE_TO_ONE,
                QueryCapabilities.conservative(),
                Optional.empty(),
                Map.of(),
                java.util.List.of()));
        return new DynamicOperationSelection(
            new PipelineReference(definition),
            "Dispatch",
            "Decide",
            runtimeStepId,
            java.util.List.of(new DynamicOperationSelection.CallableSelection(
                "lookup",
                operation,
                "LookupRequest",
                ClassName.get("com.example.common.domain", "LookupRequest"),
                "LookupResult",
                ClassName.get("com.example.common.domain", "LookupResult"),
                Map.of(),
                "sha256:catalogue")));
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
        return Jsr269GenerationContext.create(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null);
    }

    private GenerationContext generationContextWithCompilerTypes(Map<String, String> options) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        Elements elements = mock(Elements.class);
        TypeElement typeElement = mock(TypeElement.class);
        TypeMirror typeMirror = mock(TypeMirror.class);
        Types types = mock(Types.class);
        when(processingEnv.getOptions()).thenReturn(options);
        when(processingEnv.getElementUtils()).thenReturn(elements);
        when(processingEnv.getTypeUtils()).thenReturn(types);
        when(elements.getTypeElement(any(CharSequence.class))).thenReturn(typeElement);
        when(typeElement.asType()).thenReturn(typeMirror);
        when(types.getDeclaredType(any(TypeElement.class), any(TypeMirror[].class)))
            .thenReturn(mock(DeclaredType.class));
        when(types.isAssignable(any(TypeMirror.class), any(TypeMirror.class))).thenReturn(true);
        return Jsr269GenerationContext.create(
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
        return Jsr269GenerationContext.create(
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
