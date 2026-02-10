package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Public-contract tests for {@link PipelineTargetResolutionPhase}.
 */
class PipelineTargetResolutionPhaseTest {

    @Test
    void phaseHasExpectedName() {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();

        assertNotNull(phase);
        assertEquals("Pipeline Target Resolution Phase", phase.name());
    }

    @Test
    void resolvesClientRoleTargetsForAllTransportModes() throws Exception {
        assertResolvedTargets(DeploymentRole.ORCHESTRATOR_CLIENT, TransportMode.GRPC,
            Set.of(GenerationTarget.CLIENT_STEP));
        assertResolvedTargets(DeploymentRole.ORCHESTRATOR_CLIENT, TransportMode.REST,
            Set.of(GenerationTarget.REST_CLIENT_STEP));
        assertResolvedTargets(DeploymentRole.ORCHESTRATOR_CLIENT, TransportMode.LOCAL,
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP));

        assertResolvedTargets(DeploymentRole.PLUGIN_CLIENT, TransportMode.GRPC,
            Set.of(GenerationTarget.CLIENT_STEP));
        assertResolvedTargets(DeploymentRole.PLUGIN_CLIENT, TransportMode.REST,
            Set.of(GenerationTarget.REST_CLIENT_STEP));
        assertResolvedTargets(DeploymentRole.PLUGIN_CLIENT, TransportMode.LOCAL,
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP));
    }

    @Test
    void resolvesServerRoleTargetsForAllTransportModes() throws Exception {
        assertResolvedTargets(DeploymentRole.PIPELINE_SERVER, TransportMode.GRPC,
            Set.of(GenerationTarget.GRPC_SERVICE));
        assertResolvedTargets(DeploymentRole.PIPELINE_SERVER, TransportMode.REST,
            Set.of(GenerationTarget.REST_RESOURCE));
        assertResolvedTargets(DeploymentRole.PIPELINE_SERVER, TransportMode.LOCAL,
            Set.of(GenerationTarget.GRPC_SERVICE));

        assertResolvedTargets(DeploymentRole.PLUGIN_SERVER, TransportMode.GRPC,
            Set.of(GenerationTarget.GRPC_SERVICE));
        assertResolvedTargets(DeploymentRole.PLUGIN_SERVER, TransportMode.REST,
            Set.of(GenerationTarget.REST_RESOURCE));
        assertResolvedTargets(DeploymentRole.PLUGIN_SERVER, TransportMode.LOCAL,
            Set.of(GenerationTarget.GRPC_SERVICE));

        assertResolvedTargets(DeploymentRole.REST_SERVER, TransportMode.GRPC,
            Set.of(GenerationTarget.GRPC_SERVICE));
        assertResolvedTargets(DeploymentRole.REST_SERVER, TransportMode.REST,
            Set.of(GenerationTarget.REST_RESOURCE));
        assertResolvedTargets(DeploymentRole.REST_SERVER, TransportMode.LOCAL,
            Set.of(GenerationTarget.GRPC_SERVICE));
    }

    @Test
    void defaultsToGrpcWhenTransportModeIsNull() throws Exception {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        context.setStepModels(List.of(step("GrpcDefaultStep", DeploymentRole.ORCHESTRATOR_CLIENT)));
        context.setTransportMode(null);

        phase.execute(context);

        PipelineStepModel updated = context.getStepModels().getFirst();
        assertEquals(Set.of(GenerationTarget.CLIENT_STEP), updated.enabledTargets());
        assertEquals(Set.of(GenerationTarget.CLIENT_STEP), context.getResolvedTargets());
    }

    @Test
    void aggregatesResolvedTargetsAcrossModels() throws Exception {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        context.setTransportMode(TransportMode.REST);
        context.setStepModels(List.of(
            step("ServerStep", DeploymentRole.PIPELINE_SERVER),
            step("ClientStep", DeploymentRole.ORCHESTRATOR_CLIENT)));

        phase.execute(context);

        assertEquals(
            Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.REST_CLIENT_STEP),
            context.getResolvedTargets());
    }

    @Test
    void preservesModelIdentityFieldsAndOnlyReplacesEnabledTargets() throws Exception {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        PipelineStepModel original = step("IdentityStep", DeploymentRole.PIPELINE_SERVER);
        context.setStepModels(List.of(original));
        context.setTransportMode(TransportMode.REST);

        phase.execute(context);

        PipelineStepModel updated = context.getStepModels().getFirst();
        assertEquals(original.serviceName(), updated.serviceName());
        assertEquals(original.generatedName(), updated.generatedName());
        assertEquals(original.servicePackage(), updated.servicePackage());
        assertEquals(original.serviceClassName(), updated.serviceClassName());
        assertEquals(original.inputMapping(), updated.inputMapping());
        assertEquals(original.outputMapping(), updated.outputMapping());
        assertEquals(original.streamingShape(), updated.streamingShape());
        assertEquals(original.executionMode(), updated.executionMode());
        assertEquals(original.deploymentRole(), updated.deploymentRole());
        assertEquals(original.sideEffect(), updated.sideEffect());
        assertEquals(original.cacheKeyGenerator(), updated.cacheKeyGenerator());
        assertEquals(Set.of(GenerationTarget.REST_RESOURCE), updated.enabledTargets());
    }

    private void assertResolvedTargets(
            DeploymentRole role,
            TransportMode transportMode,
            Set<GenerationTarget> expectedTargets) throws Exception {
        PipelineTargetResolutionPhase phase = new PipelineTargetResolutionPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        context.setTransportMode(transportMode);
        context.setStepModels(List.of(step("Step" + role + transportMode, role)));

        phase.execute(context);

        PipelineStepModel updated = context.getStepModels().getFirst();
        assertEquals(expectedTargets, updated.enabledTargets());
        assertEquals(expectedTargets, context.getResolvedTargets());
    }

    private PipelineStepModel step(String serviceName, DeploymentRole role) {
        return new PipelineStepModel(
            serviceName,
            serviceName,
            "com.example.service",
            ClassName.get("com.example.service", serviceName),
            new TypeMapping(ClassName.get("com.example.domain", "In"), null, false),
            new TypeMapping(ClassName.get("com.example.domain", "Out"), null, false),
            StreamingShape.UNARY_UNARY,
            Set.of(GenerationTarget.GRPC_SERVICE),
            ExecutionMode.DEFAULT,
            role,
            false,
            null
        );
    }
}
