package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Map;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for the PipelineSemanticAnalysisPhase.
 */
public class PipelineSemanticAnalysisPhaseTest {

    @Test
    public void testPipelineSemanticAnalysisPhaseCreation() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        assertNotNull(phase);
        assertEquals("Pipeline Semantic Analysis Phase", phase.name());
    }

    @Test
    public void testStreamingShapeMethods() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        // Test different cardinalities
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.UNARY_STREAMING, 
                     phase.streamingShape("EXPANSION"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_UNARY, 
                     phase.streamingShape("REDUCTION"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_STREAMING, 
                     phase.streamingShape("MANY_TO_MANY"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY, 
                     phase.streamingShape("OTHER"));
    }

    @Test
    public void testIsStreamingInputCardinality() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        assertTrue(phase.isStreamingInputCardinality("REDUCTION"));
        assertTrue(phase.isStreamingInputCardinality("MANY_TO_MANY"));
        assertFalse(phase.isStreamingInputCardinality("EXPANSION"));
        assertFalse(phase.isStreamingInputCardinality("OTHER"));
    }

    @Test
    public void testApplyCardinalityToStreaming() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        // Test EXPANSION turns streaming on
        assertTrue(phase.applyCardinalityToStreaming("EXPANSION", false));
        assertTrue(phase.applyCardinalityToStreaming("EXPANSION", true));
        
        // Test MANY_TO_MANY turns streaming on
        assertTrue(phase.applyCardinalityToStreaming("MANY_TO_MANY", false));
        assertTrue(phase.applyCardinalityToStreaming("MANY_TO_MANY", true));
        
        // Test REDUCTION turns streaming off
        assertFalse(phase.applyCardinalityToStreaming("REDUCTION", false));
        assertFalse(phase.applyCardinalityToStreaming("REDUCTION", true));
        
        // Test other cardinalities preserve current state
        assertFalse(phase.applyCardinalityToStreaming("OTHER", false));
        assertTrue(phase.applyCardinalityToStreaming("OTHER", true));
    }

    @Test
    public void testIsCacheAspect() {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        
        // Create a mock aspect model for testing
        org.pipelineframework.processor.ir.PipelineAspectModel cacheAspect = 
            new org.pipelineframework.processor.ir.PipelineAspectModel(
                "cache", 
                org.pipelineframework.processor.ir.AspectScope.GLOBAL, 
                org.pipelineframework.processor.ir.AspectPosition.BEFORE_STEP, 
                java.util.Map.of()
            );
        
        org.pipelineframework.processor.ir.PipelineAspectModel otherAspect = 
            new org.pipelineframework.processor.ir.PipelineAspectModel(
                "other", 
                org.pipelineframework.processor.ir.AspectScope.GLOBAL, 
                org.pipelineframework.processor.ir.AspectPosition.BEFORE_STEP, 
                java.util.Map.of()
            );
        
        assertTrue(phase.isCacheAspect(cacheAspect));
        assertFalse(phase.isCacheAspect(otherAspect));
    }

    @Test
    void lambdaPlatformRequiresRestTransport() throws Exception {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        Messager messager = mock(Messager.class);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPlatformMode(PlatformMode.LAMBDA);
        context.setTransportMode(TransportMode.GRPC);
        context.setStepModels(List.of(step(StreamingShape.UNARY_UNARY)));

        phase.execute(context);

        verify(messager).printMessage(eq(Diagnostic.Kind.ERROR),
            contains("requires pipeline.transport=REST"));
    }

    @Test
    void lambdaPlatformRejectsStreamingShapes() throws Exception {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        Messager messager = mock(Messager.class);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPlatformMode(PlatformMode.LAMBDA);
        context.setTransportMode(TransportMode.REST);
        context.setStepModels(List.of(step(StreamingShape.UNARY_STREAMING)));

        phase.execute(context);

        verify(messager).printMessage(eq(Diagnostic.Kind.ERROR),
            contains("supports only UNARY_UNARY"));
    }

    @Test
    void lambdaPlatformAllowsUnaryRestSteps() throws Exception {
        PipelineSemanticAnalysisPhase phase = new PipelineSemanticAnalysisPhase();
        Messager messager = mock(Messager.class);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPlatformMode(PlatformMode.LAMBDA);
        context.setTransportMode(TransportMode.REST);
        context.setStepModels(List.of(step(StreamingShape.UNARY_UNARY)));

        phase.execute(context);

        verifyNoInteractions(messager);
    }

    private PipelineStepModel step(StreamingShape streamingShape) {
        return new PipelineStepModel.Builder()
            .serviceName("ProcessPaymentStatusService")
            .generatedName("ProcessPaymentStatusService")
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", "ProcessPaymentStatusService"))
            .streamingShape(streamingShape)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_RESOURCE))
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentStatus"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentStatusMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentOutput"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentOutputMapper"),
                true))
            .build();
    }
}
