package org.pipelineframework.processor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.SourceVersion;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.RestClientStepRenderer;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrchestratorClientGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesGrpcClientStepsFromTemplate() throws Exception {
        PipelineStepProcessor processor = new PipelineStepProcessor();
        ProcessingEnvironment processingEnv = mockProcessingEnv();
        processor.init(processingEnv);

        setField(processor, "generatedSourcesRoot", tempDir);
        setField(processor, "pipelineTemplateConfig", sampleConfig("GRPC"));
        setField(processor, "pipelineAspects", List.of());
        setField(processor, "transportMode", enumValue(processor, "TransportMode", "GRPC"));

        GrpcBindingResolver grpcBindingResolver = mock(GrpcBindingResolver.class);
        when(grpcBindingResolver.resolve(any(), any())).thenReturn(mock(GrpcBinding.class));
        setField(processor, "bindingResolver", grpcBindingResolver);

        ClientStepRenderer clientRenderer = mock(ClientStepRenderer.class);
        setField(processor, "clientRenderer", clientRenderer);
        RestClientStepRenderer restClientRenderer = mock(RestClientStepRenderer.class);
        setField(processor, "restClientRenderer", restClientRenderer);
        setField(processor, "grpcRenderer", mock(org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer.class));
        setField(processor, "restRenderer", mock(org.pipelineframework.processor.renderer.RestResourceRenderer.class));

        invokeGenerateClients(processor, DescriptorProtos.FileDescriptorSet.getDefaultInstance());

        verify(clientRenderer, times(2)).render(any(), any());
        verifyNoInteractions(restClientRenderer);
    }

    @Test
    void generatesRestClientStepsFromTemplate() throws Exception {
        PipelineStepProcessor processor = new PipelineStepProcessor();
        ProcessingEnvironment processingEnv = mockProcessingEnv();
        processor.init(processingEnv);

        setField(processor, "generatedSourcesRoot", tempDir);
        setField(processor, "pipelineTemplateConfig", sampleConfig("REST"));
        setField(processor, "pipelineAspects", List.of());
        setField(processor, "transportMode", enumValue(processor, "TransportMode", "REST"));

        RestBindingResolver restBindingResolver = mock(RestBindingResolver.class);
        when(restBindingResolver.resolve(any(), any())).thenReturn(mock(RestBinding.class));
        setField(processor, "restBindingResolver", restBindingResolver);

        RestClientStepRenderer restClientRenderer = mock(RestClientStepRenderer.class);
        setField(processor, "restClientRenderer", restClientRenderer);
        ClientStepRenderer clientRenderer = mock(ClientStepRenderer.class);
        setField(processor, "clientRenderer", clientRenderer);
        setField(processor, "grpcRenderer", mock(org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer.class));
        setField(processor, "restRenderer", mock(org.pipelineframework.processor.renderer.RestResourceRenderer.class));

        invokeGenerateClients(processor, DescriptorProtos.FileDescriptorSet.getDefaultInstance());

        verify(restClientRenderer, times(2)).render(any(), any());
        verifyNoInteractions(clientRenderer);
    }

    private ProcessingEnvironment mockProcessingEnv() throws Exception {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        when(processingEnv.getMessager()).thenReturn(mock(Messager.class));
        when(processingEnv.getElementUtils()).thenReturn(mock(javax.lang.model.util.Elements.class));
        when(processingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
        Files.createDirectories(tempDir);
        return processingEnv;
    }

    private PipelineTemplateConfig sampleConfig(String transport) {
        PipelineTemplateStep step1 = new PipelineTemplateStep(
            "Process Alpha",
            "ONE_TO_ONE",
            "AlphaInput",
            List.of(),
            "AlphaOutput",
            List.of());
        PipelineTemplateStep step2 = new PipelineTemplateStep(
            "Process Beta",
            "ONE_TO_ONE",
            "BetaInput",
            List.of(),
            "BetaOutput",
            List.of());
        return new PipelineTemplateConfig(
            "Sample App",
            "com.example",
            transport,
            List.of(step1, step2),
            Map.of());
    }

    private void invokeGenerateClients(PipelineStepProcessor processor,
                                       DescriptorProtos.FileDescriptorSet descriptorSet) throws Exception {
        Method method = PipelineStepProcessor.class.getDeclaredMethod(
            "generateOrchestratorClientsFromTemplate",
            DescriptorProtos.FileDescriptorSet.class);
        method.setAccessible(true);
        method.invoke(processor, descriptorSet);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Object enumValue(Object target, String enumName, String value) throws Exception {
        Class<?> enumType = null;
        for (Class<?> declared : target.getClass().getDeclaredClasses()) {
            if (declared.isEnum() && declared.getSimpleName().equals(enumName)) {
                enumType = declared;
                break;
            }
        }
        if (enumType == null) {
            throw new IllegalStateException("Enum " + enumName + " not found.");
        }
        @SuppressWarnings("unchecked")
        Object enumValue = Enum.valueOf((Class<Enum>) enumType, value);
        return enumValue;
    }

}
