package org.pipelineframework.processor;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.SourceVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.AspectPosition;
import org.pipelineframework.processor.ir.AspectScope;
import org.pipelineframework.processor.ir.PipelineAspectModel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginHostAspectFilteringTest {

    @TempDir
    Path tempDir;

    @Test
    void filtersPluginHostStepsByAnnotatedPluginName() throws Exception {
        Path pomPath = tempDir.resolve("pom.xml");
        Files.writeString(pomPath, """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>test</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);
        Path configDir = tempDir.resolve("config");
        Files.createDirectories(configDir);
        Files.writeString(configDir.resolve("pipeline.yaml"), """
            basePackage: "com.example"
            steps:
              - inputTypeName: "InType"
                outputTypeName: "OutType"
            """);
        Path generatedSourcesRoot = tempDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesRoot);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions())
            .thenReturn(Map.of("pipeline.generatedSourcesDir", generatedSourcesRoot.toString()));
        when(processingEnv.getMessager()).thenReturn(mock(Messager.class));
        when(processingEnv.getElementUtils())
            .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(processingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);

        PipelineStepProcessor processor = new PipelineStepProcessor();
        processor.init(processingEnv);

        List<PipelineAspectModel> aspects = List.of(
            new PipelineAspectModel(
                "persistence",
                AspectScope.GLOBAL,
                AspectPosition.AFTER_STEP,
                0,
                Map.of("pluginImplementationClass", "com.example.PersistenceService")),
            new PipelineAspectModel(
                "cache-invalidate",
                AspectScope.GLOBAL,
                AspectPosition.AFTER_STEP,
                1,
                Map.of("pluginImplementationClass", "com.example.CacheInvalidationService"))
        );

        Method method = PipelineStepProcessor.class.getDeclaredMethod(
            "buildPluginHostSteps",
            List.class,
            Set.class
        );
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ResolvedStep> steps = (List<ResolvedStep>) method.invoke(
            processor,
            aspects,
            Set.of("persistence")
        );

        assertEquals(1, steps.size());
        ResolvedStep resolvedStep = steps.get(0);
        assertEquals("PersistenceOutTypeSideEffect", resolvedStep.model().generatedName());
        assertTrue(resolvedStep.model().serviceName().startsWith("Observe"));
    }
}
