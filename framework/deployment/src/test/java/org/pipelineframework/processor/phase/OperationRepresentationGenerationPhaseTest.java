package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.ir.ConnectorOperationSelection;
import org.pipelineframework.processor.ir.DynamicOperationSelection;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.representation.ProviderArtifactWriter;
import org.pipelineframework.processor.representation.RepresentationProviderRegistry;
import org.pipelineframework.representation.spi.ArtifactDescription;
import org.pipelineframework.representation.spi.ArtifactKind;
import org.pipelineframework.representation.spi.ArtifactPhase;
import org.pipelineframework.representation.spi.OperationBoundaryClaim;
import org.pipelineframework.representation.spi.OperationBoundaryRequest;
import org.pipelineframework.representation.spi.OperationProviderGenerationRequest;
import org.pipelineframework.representation.spi.OperationRepresentationRequest;
import org.pipelineframework.representation.spi.ProviderMetadata;
import org.pipelineframework.representation.spi.RepresentationProvider;
import org.pipelineframework.representation.spi.ResolvedOperationRepresentation;

class OperationRepresentationGenerationPhaseTest {
    @TempDir Path tempDir;

    @Test
    void resolvesSelectedConnectorBoundariesAndDelegatesOnlyArtifactMaterialization() throws Exception {
        Path yaml = tempDir.resolve("pipeline.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: operation-representation-proof
            basePackage: example
            types:
              Input: { fields: [[subject, string]] }
              Output: { fields: [[value, string]] }
            steps: []
            """);
        var config = new PipelineTemplateConfigLoader().load(yaml);
        var provider = new RecordingProvider();
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Filer filer = mock(Filer.class);
        when(processing.getFiler()).thenReturn(filer);
        PipelineCompilationContext context = new PipelineCompilationContext(processing, mock(RoundEnvironment.class));
        context.setPipelineTemplateConfig(config);
        context.setRepresentationProviderRegistry(RepresentationProviderRegistry.of(List.of(provider)));
        context.setStepModels(List.of(model()));
        ProviderArtifactWriter writer = mock(ProviderArtifactWriter.class);

        new OperationRepresentationGenerationPhase(writer).execute(context);

        assertEquals(List.of("REQUEST:http.request", "RESPONSE:http.response"), provider.resolved);
        verify(writer).write(eq(filer), anyList());
    }

    @Test
    void resolvesDynamicCallableBoundariesWithoutDependingOnTheDecisionProducer() throws Exception {
        Path yaml = tempDir.resolve("dynamic-pipeline.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: operation-representation-proof
            basePackage: example
            types:
              Input: { fields: [[subject, string]] }
              Output: { fields: [[value, string]] }
            steps: []
            """);
        var config = new PipelineTemplateConfigLoader().load(yaml);
        var provider = new RecordingProvider();
        ProcessingEnvironment processing = mock(ProcessingEnvironment.class);
        Filer filer = mock(Filer.class);
        when(processing.getFiler()).thenReturn(filer);
        PipelineCompilationContext context = new PipelineCompilationContext(processing, mock(RoundEnvironment.class));
        context.setPipelineTemplateConfig(config);
        context.setRepresentationProviderRegistry(RepresentationProviderRegistry.of(List.of(provider)));
        context.setStepModels(List.of(dynamicModel()));

        new OperationRepresentationGenerationPhase(mock(ProviderArtifactWriter.class)).execute(context);

        assertEquals(List.of("REQUEST:http.request", "RESPONSE:http.response"), provider.resolved);
    }

    private static PipelineStepModel model() {
        ConnectorOperationSelection selection = operation();
        return new PipelineStepModel.Builder()
            .serviceName("Lookup")
            .generatedName("Lookup")
            .servicePackage("example")
            .serviceClassName(ClassName.get("example", "Lookup"))
            .inputMapping(TypeMapping.canonical(ClassName.get("example", "Input"), "Input"))
            .outputMapping(TypeMapping.canonical(ClassName.get("example", "Output"), "Output"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .connectorOperationSelection(selection)
            .build();
    }

    private static PipelineStepModel dynamicModel() {
        ConnectorOperationSelection selection = operation();
        DynamicOperationSelection dynamic = new DynamicOperationSelection(
            new PipelineReference("proof/callable-loop"), "Dispatch", "Decide", "proof/callable-loop#Dispatch",
            List.of(new DynamicOperationSelection.CallableSelection(
                "lookup", selection, "Input", ClassName.get("example", "Input"),
                "Output", ClassName.get("example", "Output"), Map.of(), "4".repeat(64))));
        return new PipelineStepModel.Builder()
            .serviceName("Dispatch")
            .generatedName("Dispatch")
            .servicePackage("example")
            .serviceClassName(ClassName.get("example", "Dispatch"))
            .inputMapping(TypeMapping.canonical(ClassName.get("example", "Input"), "Input"))
            .outputMapping(TypeMapping.canonical(ClassName.get("example", "Output"), "Output"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .dynamicOperationSelection(dynamic)
            .build();
    }

    private static ConnectorOperationSelection operation() {
        return ConnectorOperationSelection.query("Lookup",
            ConnectorBindingName.of("vendor-http"),
            new ConnectorOperationIdentity(ConnectorProviderId.of("http.client"), "evidence.lookup",
                ConnectorOperationKind.QUERY, 1),
            1, Map.of(), new ConnectorOperationSelection.QuerySelection(QueryOperationCardinality.ONE_TO_ONE,
                QueryCapabilities.conservative(), Optional.empty(), Map.of(), List.of()));
    }

    private static final class RecordingProvider implements RepresentationProvider {
        private final List<String> resolved = new ArrayList<>();

        @Override
        public ProviderMetadata metadata() {
            return new ProviderMetadata("http", Set.of(), Set.of("connector-operation"));
        }

        @Override
        public boolean supportsOperationProvider(String connectorProviderId, int connectorProviderMajorVersion) {
            return "http.client".equals(connectorProviderId) && connectorProviderMajorVersion == 1;
        }

        @Override
        public Optional<OperationBoundaryClaim> claimOperation(OperationBoundaryRequest request) {
            var requestWire = wire("http.request");
            return Optional.of(new OperationBoundaryClaim("http", requestWire, List.of(wire("http.response"))));
        }

        @Override
        public Optional<ResolvedOperationRepresentation> resolveOperation(OperationRepresentationRequest request) {
            resolved.add(request.role() + ":" + request.wireBoundary().mappingKey());
            return Optional.of(new ResolvedOperationRepresentation("http", request.boundary().boundaryIdentity(),
                request.role(), request.wireBoundary().mappingKey(), request.canonicalType(), "DIRECT",
                Optional.empty(), Optional.empty(), "1".repeat(64), Map.of()));
        }

        @Override
        public List<ArtifactDescription> describeOperationArtifacts(OperationProviderGenerationRequest request) {
            assertEquals(2, request.representations().size());
            return List.of(new ArtifactDescription("http", ArtifactPhase.RESOURCE, ArtifactKind.RESOURCE,
                "META-INF/pipeline/http-operation-bindings.json", "{}", 0));
        }

        private static OperationBoundaryClaim.WireBoundary wire(String key) {
            return new OperationBoundaryClaim.WireBoundary(key,
                "{\"additionalProperties\":false,\"properties\":{},\"type\":\"object\"}", "1".repeat(64));
        }
    }
}
