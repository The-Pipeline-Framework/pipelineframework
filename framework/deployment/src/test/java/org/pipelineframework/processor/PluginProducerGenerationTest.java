package org.pipelineframework.processor;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginProducerGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesSideEffectBeanForParameterizedPlugin() throws Exception {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ObserveOutputTypeSideEffectService")
            .generatedName("PersistenceOutputTypeSideEffect")
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("org.pipelineframework.plugin.persistence", "PersistenceService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.domain", "OutputType"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.domain", "OutputType"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .enabledTargets(java.util.Set.of(GenerationTarget.GRPC_SERVICE))
            .deploymentRole(DeploymentRole.PLUGIN_SERVER)
            .sideEffect(true)
            .build();

        PipelineStepProcessor processor = new PipelineStepProcessor();
        Method method = PipelineStepProcessor.class.getDeclaredMethod(
            "generateSideEffectBean",
            PipelineStepModel.class,
            Path.class
        );
        method.setAccessible(true);
        method.invoke(processor, model, tempDir);

        Path generated = tempDir.resolve("com/example/service/pipeline/PersistenceServiceOutputTypeSideEffectBean.java");
        String source = Files.readString(generated);
        assertTrue(source.contains("ApplicationScoped"));
        assertTrue(source.contains("extends PersistenceService<OutputType>"));
    }
}
