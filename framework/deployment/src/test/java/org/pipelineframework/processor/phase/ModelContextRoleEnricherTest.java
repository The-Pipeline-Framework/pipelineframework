package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.AspectPosition;
import org.pipelineframework.processor.ir.AspectScope;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ModelContextRoleEnricherTest {

    private final ModelContextRoleEnricher enricher = new ModelContextRoleEnricher();

    @Test
    void enrichReturnsEmptyWhenNoPluginHostAndNoOrchestrator() {
        PipelineCompilationContext ctx = new PipelineCompilationContext(null, null);
        List<PipelineStepModel> result = enricher.enrich(ctx, List.of(step("ProcessAService", false)));
        assertTrue(result.isEmpty());
    }

    @Test
    void enrichProducesPluginAndClientModelsForColocatedPluginHost() {
        PipelineCompilationContext ctx = new PipelineCompilationContext(null, null);
        ctx.setPluginHost(true);
        ctx.setTransportMode(PipelineTransport.LOCAL);
        ctx.setAspectModels(List.of());

        List<PipelineStepModel> result = enricher.enrich(
            ctx,
            List.of(step("ProcessAService", true), step("ProcessBService", false)));

        assertEquals(3, result.size());
        assertEquals(DeploymentRole.PLUGIN_SERVER, result.get(0).deploymentRole());
        assertEquals("ProcessAService", result.get(0).serviceName());
        assertEquals(DeploymentRole.ORCHESTRATOR_CLIENT, result.get(1).deploymentRole());
        assertEquals(DeploymentRole.ORCHESTRATOR_CLIENT, result.get(2).deploymentRole());
    }

    @Test
    void enrichReturnsEmptyForDistributedPluginHostWithoutPluginAspectBindings() {
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        when(roundEnv.getElementsAnnotatedWith(org.pipelineframework.annotation.PipelinePlugin.class))
            .thenReturn(Set.of());
        when(roundEnv.getElementsAnnotatedWith(org.pipelineframework.annotation.PipelineOrchestrator.class))
            .thenReturn(Set.of());

        PipelineCompilationContext ctx = new PipelineCompilationContext(null, roundEnv);
        ctx.setPluginHost(true);
        ctx.setTransportMode(PipelineTransport.GRPC);
        ctx.setAspectModels(List.of(new PipelineAspectModel(
            "audit",
            AspectScope.GLOBAL,
            AspectPosition.AFTER_STEP,
            Map.of("pluginImplementationClass", "com.example.AuditPlugin"))));

        List<PipelineStepModel> result = enricher.enrich(ctx, List.of(step("ProcessAService", true)));
        assertTrue(result.isEmpty());
    }

    @Test
    void enrichKeepsServerModelsForRuntimeMappedModularStepModule() {
        PipelineCompilationContext ctx = new PipelineCompilationContext(null, null);
        ctx.setModuleName("crawl-source-svc");
        ctx.setRuntimeMapping(new PipelineRuntimeMapping(
            PipelineRuntimeMapping.Layout.MODULAR,
            PipelineRuntimeMapping.Validation.STRICT,
            PipelineRuntimeMapping.Defaults.defaultValues(),
            Map.of("lambda", "lambda"),
            Map.of("crawl-source-svc", "lambda"),
            Map.of("ProcessAService", "crawl-source-svc"),
            Map.of()));

        List<PipelineStepModel> result = enricher.enrich(
            ctx,
            List.of(
                step("ProcessAService", false),
                step("ProcessBService", false).toBuilder()
                    .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
                    .build()));

        assertEquals(1, result.size());
        assertEquals("ProcessAService", result.get(0).serviceName());
        assertEquals(DeploymentRole.PIPELINE_SERVER, result.get(0).deploymentRole());
    }

    @Test
    void enrichProducesServerAndClientModelsForMonolithLayout() {
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        doReturn(Set.of(mock(Element.class)))
            .when(roundEnv)
            .getElementsAnnotatedWith(eq(PipelineOrchestrator.class));

        PipelineCompilationContext ctx = new PipelineCompilationContext(null, roundEnv);
        ctx.setRuntimeMapping(new PipelineRuntimeMapping(
            PipelineRuntimeMapping.Layout.MONOLITH,
            PipelineRuntimeMapping.Validation.STRICT,
            PipelineRuntimeMapping.Defaults.defaultValues(),
            Map.of("monolith-svc", "monolith-svc"),
            Map.of("monolith-svc", "monolith-svc"),
            Map.of("ProcessAService", "monolith-svc"),
            Map.of()));

        List<PipelineStepModel> result = enricher.enrich(ctx, List.of(step("ProcessAService", false)));

        assertEquals(2, result.size());
        assertEquals(DeploymentRole.PIPELINE_SERVER, result.get(0).deploymentRole());
        assertEquals(DeploymentRole.ORCHESTRATOR_CLIENT, result.get(1).deploymentRole());
        assertEquals("ProcessAService", result.get(0).serviceName());
        assertEquals("ProcessAService", result.get(1).serviceName());
    }

    @Test
    void enrichKeepsAwaitStepsClientOnlyForMonolithLayout() {
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        doReturn(Set.of(mock(Element.class)))
            .when(roundEnv)
            .getElementsAnnotatedWith(eq(PipelineOrchestrator.class));

        PipelineCompilationContext ctx = new PipelineCompilationContext(null, roundEnv);
        ctx.setRuntimeMapping(new PipelineRuntimeMapping(
            PipelineRuntimeMapping.Layout.MONOLITH,
            PipelineRuntimeMapping.Validation.STRICT,
            PipelineRuntimeMapping.Defaults.defaultValues(),
            Map.of("monolith-svc", "monolith-svc"),
            Map.of("monolith-svc", "monolith-svc"),
            Map.of(
                "ProcessValidateOrderRequestService", "monolith-svc",
                "ProcessAwaitRestaurantDecisionService", "monolith-svc"),
            Map.of()));

        List<PipelineStepModel> result = enricher.enrich(
            ctx,
            List.of(
                step("ProcessValidateOrderRequestService", false),
                awaitStep("ProcessAwaitRestaurantDecisionService")));

        assertEquals(3, result.size());
        assertEquals(DeploymentRole.PIPELINE_SERVER, result.get(0).deploymentRole());
        assertEquals("ProcessValidateOrderRequestService", result.get(0).serviceName());
        assertEquals(DeploymentRole.ORCHESTRATOR_CLIENT, result.get(1).deploymentRole());
        assertEquals("ProcessValidateOrderRequestService", result.get(1).serviceName());
        assertEquals(DeploymentRole.ORCHESTRATOR_CLIENT, result.get(2).deploymentRole());
        assertEquals("ProcessAwaitRestaurantDecisionService", result.get(2).serviceName());
    }

    private PipelineStepModel step(String serviceName, boolean sideEffect) {
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage("com.example.pipeline")
            .serviceClassName(ClassName.get("com.example.pipeline", serviceName))
            .inputMapping(new TypeMapping(null, null, false))
            .outputMapping(new TypeMapping(null, null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(sideEffect)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }

    private PipelineStepModel awaitStep(String serviceName) {
        return new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .generatedName(serviceName)
            .servicePackage("org.pipelineframework.restaurantapproval.service")
            .serviceClassName(ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"))
            .inputMapping(new TypeMapping(null, null, false))
            .outputMapping(new TypeMapping(null, null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.AWAIT_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .cacheKeyGenerator(null)
            .orderingRequirement(OrderingRequirement.RELAXED)
            .threadSafety(ThreadSafety.SAFE)
            .build();
    }
}
