package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MockitoExtension.class)
class StepBindingConstructionServiceTest {

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private Messager messager;

    private StepBindingConstructionService service;

    @BeforeEach
    void setUp() {
        service = new StepBindingConstructionService();
    }

    @Test
    void constructorRejectsNullDependencies() {
        assertThrows(NullPointerException.class, () -> new StepBindingConstructionService(null, new org.pipelineframework.processor.util.RestBindingResolver()));
        assertThrows(NullPointerException.class, () -> new StepBindingConstructionService(new org.pipelineframework.processor.util.GrpcBindingResolver(), null));
    }

    @Test
    void delegatedStepBuildsExternalAdapterAndLocalBinding() {
        when(processingEnv.getMessager()).thenReturn(messager);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, null);
        PipelineStepModel delegated = TestModelFactory
            .createTestModelWithTargets("DelegatedService", Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .toBuilder()
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .build();
        ctx.setStepModels(List.of(delegated));

        Map<String, Object> bindings = service.buildBindings(ctx, null);

        assertTrue(bindings.containsKey("DelegatedService_external_adapter"));
        assertTrue(bindings.containsKey("DelegatedService_local"));
        assertFalse(bindings.containsKey("DelegatedService_grpc"));
    }

    @Test
    void delegatedStepWithServerTargetsEmitsWarning() {
        when(processingEnv.getMessager()).thenReturn(messager);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, null);
        PipelineStepModel delegated = TestModelFactory
            .createTestModelWithTargets("DelegatedWarnService", Set.of(GenerationTarget.GRPC_SERVICE))
            .toBuilder()
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .build();
        ctx.setStepModels(List.of(delegated));

        service.buildBindings(ctx, null);

        verify(messager).printMessage(
            eq(javax.tools.Diagnostic.Kind.WARNING),
            contains("Delegated step 'DelegatedWarnService' ignores server targets"));
    }
}
