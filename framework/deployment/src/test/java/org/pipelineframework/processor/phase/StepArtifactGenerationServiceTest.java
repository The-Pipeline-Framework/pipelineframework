package org.pipelineframework.processor.phase;

import java.nio.file.Path;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StepArtifactGenerationServiceTest {

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private Messager messager;

    private StepArtifactGenerationService service;

    @BeforeEach
    void setUp() {
        service = new StepArtifactGenerationService(
            new GenerationPathResolver(),
            new GenerationPolicy(),
            new SideEffectBeanService(new GenerationPathResolver()));
    }

    @Test
    void constructorRejectsNullDependencies() {
        assertThrows(NullPointerException.class,
            () -> new StepArtifactGenerationService(null, new GenerationPolicy(), new SideEffectBeanService(new GenerationPathResolver())));
        assertThrows(NullPointerException.class,
            () -> new StepArtifactGenerationService(new GenerationPathResolver(), null, new SideEffectBeanService(new GenerationPathResolver())));
        assertThrows(NullPointerException.class,
            () -> new StepArtifactGenerationService(new GenerationPathResolver(), new GenerationPolicy(), null));
    }

    @Test
    void clientStepWithoutGrpcBindingIsSkipped() {
        when(processingEnv.getMessager()).thenReturn(messager);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, null);
        ctx.setGeneratedSourcesRoot(Path.of("target/generated-sources-test"));

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessMissingBindingService")
            .generatedName("ProcessMissingBindingService")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("com.example", "MissingBindingService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example", "In"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example", "Out"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .build();

        assertDoesNotThrow(() -> invokeGenerateArtifacts(ctx, model));

        verify(messager).printMessage(
            eq(javax.tools.Diagnostic.Kind.WARNING),
            contains("Skipping gRPC client step generation"));
    }

    private void invokeGenerateArtifacts(PipelineCompilationContext ctx, PipelineStepModel model) throws Exception {
        service.generateArtifactsForModel(
            ctx,
            model,
            null,
            null,
            null,
            new java.util.HashSet<>(),
            Set.of(),
            null,
            null,
            new RoleMetadataGenerator(processingEnv),
            mock(org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer.class),
            mock(org.pipelineframework.processor.renderer.ClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.LocalClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestResourceRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestFunctionHandlerRenderer.class)
        );
    }
}
