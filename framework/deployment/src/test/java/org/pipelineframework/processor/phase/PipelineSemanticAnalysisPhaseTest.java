package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for the PipelineSemanticAnalysisPhase.
 */
public class PipelineSemanticAnalysisPhaseTest {
    private PipelineSemanticAnalysisPhase phase;
    private Messager messager;
    private ProcessingEnvironment processingEnv;
    private RoundEnvironment roundEnv;

    @BeforeEach
    public void setUp() {
        phase = new PipelineSemanticAnalysisPhase();
        messager = mock(Messager.class);
        processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        roundEnv = mock(RoundEnvironment.class);
    }

    @Test
    public void testPipelineSemanticAnalysisPhaseCreation() {
        assertNotNull(phase);
        assertEquals("Pipeline Semantic Analysis Phase", phase.name());
    }

    @Test
    public void testStreamingShapeMethods() {
        // Test different cardinalities
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.UNARY_STREAMING, 
                     phase.streamingShape("EXPANSION"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.UNARY_STREAMING,
                     phase.streamingShape("ONE_TO_MANY"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_UNARY, 
                     phase.streamingShape("REDUCTION"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_UNARY,
                     phase.streamingShape("MANY_TO_ONE"));
        assertEquals(org.pipelineframework.processor.ir.StreamingShape.STREAMING_STREAMING, 
                     phase.streamingShape("MANY_TO_MANY"));
        assertThrows(IllegalArgumentException.class, () -> phase.streamingShape("OTHER"));
    }

    @Test
    public void testIsStreamingInputCardinality() {
        assertTrue(phase.isStreamingInputCardinality("REDUCTION"));
        assertTrue(phase.isStreamingInputCardinality("MANY_TO_ONE"));
        assertTrue(phase.isStreamingInputCardinality("MANY_TO_MANY"));
        assertFalse(phase.isStreamingInputCardinality("EXPANSION"));
        assertThrows(IllegalArgumentException.class, () -> phase.isStreamingInputCardinality("OTHER"));
    }

    @Test
    public void testApplyCardinalityToStreaming() {
        // Test EXPANSION turns streaming on
        assertTrue(phase.applyCardinalityToStreaming("EXPANSION", false));
        assertTrue(phase.applyCardinalityToStreaming("EXPANSION", true));
        assertTrue(phase.applyCardinalityToStreaming("ONE_TO_MANY", false));
        assertTrue(phase.applyCardinalityToStreaming("ONE_TO_MANY", true));
        
        // Test MANY_TO_MANY turns streaming on
        assertTrue(phase.applyCardinalityToStreaming("MANY_TO_MANY", false));
        assertTrue(phase.applyCardinalityToStreaming("MANY_TO_MANY", true));
        
        // Test REDUCTION turns streaming off
        assertFalse(phase.applyCardinalityToStreaming("REDUCTION", false));
        assertFalse(phase.applyCardinalityToStreaming("REDUCTION", true));
        assertFalse(phase.applyCardinalityToStreaming("MANY_TO_ONE", false));
        assertFalse(phase.applyCardinalityToStreaming("MANY_TO_ONE", true));
        
        assertThrows(IllegalArgumentException.class, () -> phase.applyCardinalityToStreaming("OTHER", false));
        assertThrows(IllegalArgumentException.class, () -> phase.applyCardinalityToStreaming("OTHER", true));
        assertThrows(IllegalArgumentException.class, () -> phase.applyCardinalityToStreaming(null, false));
        assertThrows(IllegalArgumentException.class, () -> phase.applyCardinalityToStreaming(null, true));
        assertThrows(IllegalArgumentException.class, () -> phase.applyCardinalityToStreaming("", false));
        assertThrows(IllegalArgumentException.class, () -> phase.applyCardinalityToStreaming("", true));
    }

    @Test
    public void testIsCacheAspect() {
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
    public void functionPlatformRequiresRestTransport() throws Exception {
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPlatformMode(PlatformMode.FUNCTION);
        context.setTransportMode(TransportMode.GRPC);
        context.setStepModels(List.of(step(StreamingShape.UNARY_UNARY)));

        phase.execute(context);

        verify(messager).printMessage(eq(Diagnostic.Kind.ERROR),
            contains("requires pipeline.transport=REST"));
    }

    @ParameterizedTest
    @EnumSource(
        value = StreamingShape.class,
        names = {"UNARY_STREAMING", "STREAMING_UNARY", "STREAMING_STREAMING"})
    public void functionPlatformAllowsStreamingShapes(StreamingShape shape) throws Exception {
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPlatformMode(PlatformMode.FUNCTION);
        context.setTransportMode(TransportMode.REST);
        context.setStepModels(List.of(step(shape)));

        phase.execute(context);

        verify(messager, never()).printMessage(eq(Diagnostic.Kind.ERROR), any());
    }

    @Test
    public void functionPlatformAllowsUnaryRestSteps() throws Exception {
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setPlatformMode(PlatformMode.FUNCTION);
        context.setTransportMode(TransportMode.REST);
        context.setStepModels(List.of(step(StreamingShape.UNARY_UNARY)));

        phase.execute(context);

        verify(messager, never()).printMessage(eq(Diagnostic.Kind.ERROR), any());
    }

    @Test
    public void delegatedStepWithoutMapperFailsWhenYamlTypesDifferFromDelegate() throws Exception {
        Elements elementUtils = mock(Elements.class);
        Types typeUtils = mock(Types.class);
        when(processingEnv.getElementUtils()).thenReturn(elementUtils);
        when(processingEnv.getTypeUtils()).thenReturn(typeUtils);
        when(roundEnv.getElementsAnnotatedWith(org.pipelineframework.annotation.PipelineStep.class)).thenReturn(Set.of());

        TypeElement delegateElement = mock(TypeElement.class);
        TypeElement reactiveServiceElement = mock(TypeElement.class);
        DeclaredType delegateDeclaredType = mock(DeclaredType.class);
        DeclaredType reactiveDeclaredType = mock(DeclaredType.class);
        TypeMirror delegateInputType = mock(TypeMirror.class);
        TypeMirror delegateOutputType = mock(TypeMirror.class);

        when(delegateElement.asType()).thenReturn(delegateDeclaredType);
        when(delegateElement.getQualifiedName()).thenReturn(name("com.example.lib.EmbeddingService"));
        when(reactiveServiceElement.asType()).thenReturn(reactiveDeclaredType);
        when(reactiveServiceElement.getQualifiedName()).thenReturn(name("org.pipelineframework.service.ReactiveService"));

        when(elementUtils.getTypeElement("com.example.lib.EmbeddingService")).thenReturn(delegateElement);
        when(elementUtils.getTypeElement("org.pipelineframework.service.ReactiveService")).thenReturn(reactiveServiceElement);

        when(typeUtils.isAssignable(delegateDeclaredType, reactiveDeclaredType)).thenReturn(true);
        doReturn(List.of(reactiveDeclaredType)).when(typeUtils).directSupertypes(delegateDeclaredType);
        doReturn(List.of()).when(typeUtils).directSupertypes(reactiveDeclaredType);

        when(delegateDeclaredType.asElement()).thenReturn(delegateElement);
        when(reactiveDeclaredType.asElement()).thenReturn(reactiveServiceElement);
        doReturn(List.of(delegateInputType, delegateOutputType)).when(reactiveDeclaredType).getTypeArguments();
        when(delegateInputType.toString()).thenReturn("com.example.lib.LibraryChunk");
        when(delegateOutputType.toString()).thenReturn("com.example.lib.LibraryVector");

        PipelineStepModel delegatedModel = new PipelineStepModel.Builder()
            .serviceName("ProcessEmbedService")
            .generatedName("ProcessEmbedService")
            .servicePackage("com.example.app")
            .serviceClassName(ClassName.get("com.example.app", "ProcessEmbedService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .enabledTargets(Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .inputMapping(new TypeMapping(ClassName.get("com.example.app", "TextChunk"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.app", "Vector"), null, false))
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .externalMapper(null)
            .build();

        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setStepModels(List.of(delegatedModel));

        phase.execute(context);

        verify(messager).printMessage(
            eq(Diagnostic.Kind.ERROR),
            contains("requires an external mapper"));
    }

    @Test
    public void delegatedStepFailsWhenDelegateImplementsMultipleReactiveInterfaces() throws Exception {
        Elements elementUtils = mock(Elements.class);
        Types typeUtils = mock(Types.class);
        when(processingEnv.getElementUtils()).thenReturn(elementUtils);
        when(processingEnv.getTypeUtils()).thenReturn(typeUtils);
        when(roundEnv.getElementsAnnotatedWith(org.pipelineframework.annotation.PipelineStep.class)).thenReturn(Set.of());

        TypeElement delegateElement = mock(TypeElement.class);
        TypeElement reactiveServiceElement = mock(TypeElement.class);
        TypeElement reactiveStreamingElement = mock(TypeElement.class);
        DeclaredType delegateDeclaredType = mock(DeclaredType.class);
        DeclaredType reactiveDeclaredType = mock(DeclaredType.class);
        DeclaredType reactiveStreamingDeclaredType = mock(DeclaredType.class);
        TypeMirror delegateInputType = mock(TypeMirror.class);
        TypeMirror delegateOutputType = mock(TypeMirror.class);
        TypeMirror streamingInputType = mock(TypeMirror.class);
        TypeMirror streamingOutputType = mock(TypeMirror.class);

        when(delegateElement.asType()).thenReturn(delegateDeclaredType);
        when(delegateElement.getQualifiedName()).thenReturn(name("com.example.lib.EmbeddingService"));
        when(reactiveServiceElement.asType()).thenReturn(reactiveDeclaredType);
        when(reactiveServiceElement.getQualifiedName()).thenReturn(name("org.pipelineframework.service.ReactiveService"));
        when(reactiveStreamingElement.asType()).thenReturn(reactiveStreamingDeclaredType);
        when(reactiveStreamingElement.getQualifiedName()).thenReturn(name("org.pipelineframework.service.ReactiveStreamingService"));

        when(elementUtils.getTypeElement("com.example.lib.EmbeddingService")).thenReturn(delegateElement);
        when(elementUtils.getTypeElement("org.pipelineframework.service.ReactiveService")).thenReturn(reactiveServiceElement);
        when(elementUtils.getTypeElement("org.pipelineframework.service.ReactiveStreamingService")).thenReturn(reactiveStreamingElement);

        when(typeUtils.isAssignable(delegateDeclaredType, reactiveDeclaredType)).thenReturn(true);
        when(typeUtils.isAssignable(delegateDeclaredType, reactiveStreamingDeclaredType)).thenReturn(true);
        doReturn(List.of(reactiveDeclaredType, reactiveStreamingDeclaredType))
            .when(typeUtils).directSupertypes(delegateDeclaredType);
        doReturn(List.of()).when(typeUtils).directSupertypes(reactiveDeclaredType);
        doReturn(List.of()).when(typeUtils).directSupertypes(reactiveStreamingDeclaredType);

        when(reactiveDeclaredType.asElement()).thenReturn(reactiveServiceElement);
        when(reactiveStreamingDeclaredType.asElement()).thenReturn(reactiveStreamingElement);
        doReturn(List.of(delegateInputType, delegateOutputType)).when(reactiveDeclaredType).getTypeArguments();
        doReturn(List.of(streamingInputType, streamingOutputType)).when(reactiveStreamingDeclaredType).getTypeArguments();
        when(delegateInputType.toString()).thenReturn("com.example.lib.LibraryChunk");
        when(delegateOutputType.toString()).thenReturn("com.example.lib.LibraryVector");
        when(streamingInputType.toString()).thenReturn("com.example.lib.LibraryChunk");
        when(streamingOutputType.toString()).thenReturn("com.example.lib.LibraryVector");

        PipelineStepModel delegatedModel = new PipelineStepModel.Builder()
            .serviceName("ProcessEmbedService")
            .generatedName("ProcessEmbedService")
            .servicePackage("com.example.app")
            .serviceClassName(ClassName.get("com.example.app", "ProcessEmbedService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .enabledTargets(Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .inputMapping(new TypeMapping(ClassName.get("com.example.lib", "LibraryChunk"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.lib", "LibraryVector"), null, false))
            .delegateService(ClassName.get("com.example.lib", "EmbeddingService"))
            .externalMapper(null)
            .build();

        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        context.setStepModels(List.of(delegatedModel));

        phase.execute(context);

        verify(messager).printMessage(
            eq(Diagnostic.Kind.ERROR),
            contains("implements multiple reactive service interfaces"));
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
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE))
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

    private static Name name(String value) {
        return new Name() {
            @Override
            public boolean contentEquals(CharSequence cs) {
                return value.contentEquals(cs);
            }

            @Override
            public int length() {
                return value.length();
            }

            @Override
            public char charAt(int index) {
                return value.charAt(index);
            }

            @Override
            public CharSequence subSequence(int start, int end) {
                return value.subSequence(start, end);
            }

            @Override
            public String toString() {
                return value;
            }
        };
    }
}
