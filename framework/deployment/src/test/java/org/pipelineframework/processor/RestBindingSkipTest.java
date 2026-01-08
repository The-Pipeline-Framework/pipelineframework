package org.pipelineframework.processor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.SourceVersion;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RestBindingSkipTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesGrpcAndClientWhenRestBindingMissing() throws Exception {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        when(processingEnv.getMessager()).thenReturn(mock(Messager.class));
        when(processingEnv.getElementUtils())
            .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(processingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);

        PipelineStepProcessor processor = new PipelineStepProcessor();
        processor.init(processingEnv);

        GrpcServiceAdapterRenderer grpcRenderer = mock(GrpcServiceAdapterRenderer.class);
        ClientStepRenderer clientRenderer = mock(ClientStepRenderer.class);
        RestResourceRenderer restRenderer = mock(RestResourceRenderer.class);
        RoleMetadataGenerator roleMetadataGenerator = mock(RoleMetadataGenerator.class);

        setField(processor, "grpcRenderer", grpcRenderer);
        setField(processor, "clientRenderer", clientRenderer);
        setField(processor, "restRenderer", restRenderer);
        setField(processor, "roleMetadataGenerator", roleMetadataGenerator);

        Path generatedSourcesRoot = tempDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesRoot);
        setField(processor, "generatedSourcesRoot", generatedSourcesRoot);
        setField(processor, "pipelineAspects", java.util.List.of());

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessFooService")
            .generatedName("ProcessFooService")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("com.example", "ProcessFooService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .enabledTargets(Set.of(
                GenerationTarget.GRPC_SERVICE,
                GenerationTarget.CLIENT_STEP,
                GenerationTarget.REST_RESOURCE))
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .build();

        GrpcBinding grpcBinding = new GrpcBinding(model, new Object(), new Object());

        Method method = PipelineStepProcessor.class.getDeclaredMethod(
            "generateArtifacts",
            PipelineStepModel.class,
            GrpcBinding.class,
            RestBinding.class,
            com.google.protobuf.DescriptorProtos.FileDescriptorSet.class
        );
        method.setAccessible(true);
        method.invoke(processor, model, grpcBinding, null, null);

        verify(grpcRenderer).render(any(), any());
        verify(clientRenderer).render(any(), any());
        verify(restRenderer, never()).render(any(), any());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
