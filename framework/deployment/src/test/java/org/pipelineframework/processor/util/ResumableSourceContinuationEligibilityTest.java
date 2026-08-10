package org.pipelineframework.processor.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.squareup.javapoet.ClassName;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderCapability;
import org.pipelineframework.representation.spi.ProviderExecutionStyle;
import org.pipelineframework.representation.spi.ProviderStepContract;

class ResumableSourceContinuationEligibilityTest {

    @Test
    void resolvesProviderBoundaryByGeneratedFacadeAfterYamlNamedBoundaryWasReplaced() {
        PipelineStepModel generatedFacadeModel = model(
            "ProcessCsvPaymentsInputService", "ProcessCsvPaymentsInputServicePipelineFacade",
            StreamingShape.UNARY_STREAMING);
        ResolvedProviderBoundary boundary = boundary("Process Csv Payments Input");

        Optional<ResolvedProviderBoundary> found = ResumableSourceContinuationEligibility.providerBoundary(
            generatedFacadeModel, List.of(boundary));

        assertEquals(boundary, found.orElseThrow());
        assertTrue(ResumableSourceContinuationEligibility.candidate(
            List.of(generatedFacadeModel, await()), 0, found).isPresent());
    }

    private static PipelineStepModel model(String name, String facade, StreamingShape shape) {
        ClassName type = ClassName.get(String.class);
        return new PipelineStepModel.Builder().serviceName(name).generatedName(name)
            .servicePackage("org.pipelineframework.fixture")
            .serviceClassName(ClassName.get("org.pipelineframework.fixture", facade))
            .inputMapping(new TypeMapping(type, null, false)).outputMapping(new TypeMapping(type, null, false))
            .streamingShape(shape).enabledTargets(Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT).deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false).orderingRequirement(OrderingRequirement.RELAXED).threadSafety(ThreadSafety.SAFE).build();
    }

    private static PipelineStepModel await() {
        ClassName type = ClassName.get(String.class);
        return new PipelineStepModel.Builder().serviceName("AwaitPaymentProvider").generatedName("AwaitPaymentProvider")
            .servicePackage("org.pipelineframework.fixture")
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .inputMapping(new TypeMapping(type, null, false)).outputMapping(new TypeMapping(type, null, false))
            .streamingShape(StreamingShape.UNARY_UNARY).enabledTargets(Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT).deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false).orderingRequirement(OrderingRequirement.RELAXED).threadSafety(ThreadSafety.SAFE).build();
    }

    private static ResolvedProviderBoundary boundary(String yamlStepName) {
        CanonicalType string = new CanonicalType("String", String.class.getName(), CanonicalTypeShape.RECORD);
        return new ResolvedProviderBoundary(new BoundaryRequest(yamlStepName, "org.pipelineframework.fixture.CsvReader", string,
            string, "UNARY_STREAMING", Set.of(), Map.of()), new BoundaryClaim("opencsv", "binding",
            "org.pipelineframework.fixture.ProcessCsvPaymentsInputServicePipelineFacade", Optional.of(
                new ProviderStepContract(ProviderExecutionStyle.BLOCKING_ITERATOR, "UNARY_STREAMING",
                    Set.of(ProviderCapability.RESUMABLE_SOURCE)))), List.of(), Map.of());
    }
}
