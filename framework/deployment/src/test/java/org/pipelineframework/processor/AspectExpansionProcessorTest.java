package org.pipelineframework.processor;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.util.GrpcBindingResolver;

import static org.junit.jupiter.api.Assertions.*;

class AspectExpansionProcessorTest {

    @Test
    void expandsGlobalAspectsAfterSteps() throws Exception {
        ResolvedStep step = resolvedStep("ProcessFolderService");
        PipelineAspectModel aspect = new PipelineAspectModel(
            "persistence",
            AspectScope.GLOBAL,
            AspectPosition.AFTER_STEP,
            aspectConfig("org.pipelineframework.plugin.persistence.PersistenceService")
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(
            List.of(step),
            List.of(aspect)
        );

        assertEquals(2, expanded.size(), "Expected original step and one synthetic step");
        assertEquals(step.model().serviceName(), expanded.get(0).model().serviceName());
        assertTrue(expanded.get(1).model().generatedName().contains("Persistence"));
        assertEquals("ObservePersistenceCsvPaymentsInputFileSideEffectService", expanded.get(1).model().serviceName());
    }

    @SneakyThrows
    @Test
    void rejectsStepScopedAspectTargetingMissingStep() {
        ResolvedStep step = resolvedStep("ProcessFolderService");
        ResolvedStep extraStep = resolvedStepWithoutBinding("ProcessPaymentsService");
        Map<String, Object> config = aspectConfig("org.pipelineframework.plugin.persistence.PersistenceService");
        config.put("targetSteps", List.of("MissingStep"));

        PipelineAspectModel stepScoped = new PipelineAspectModel(
            "persistence",
            AspectScope.STEPS,
            AspectPosition.AFTER_STEP,
            config
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        assertThrows(IllegalArgumentException.class, () -> processor.expandAspects(
            List.of(step, extraStep),
            List.of(stepScoped)
        ));
    }

    @SneakyThrows
    @Test
    void appliesStepScopedAspectWhenTargetStepsProvidedAsList() {
        ResolvedStep step = resolvedStep("ProcessFolderService");
        Map<String, Object> config = aspectConfig("org.pipelineframework.plugin.persistence.PersistenceService");
        config.put("targetSteps", List.of("ProcessFolderService"));

        PipelineAspectModel stepScoped = new PipelineAspectModel(
            "persistence",
            AspectScope.STEPS,
            AspectPosition.AFTER_STEP,
            config
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(
            List.of(step),
            List.of(stepScoped)
        );

        assertEquals(2, expanded.size(), "Expected original step and one synthetic step");
        assertEquals(step.model().serviceName(), expanded.get(0).model().serviceName());
        assertTrue(expanded.get(1).model().generatedName().contains("Persistence"));
    }

    @SneakyThrows
    @Test
    void assignsPluginRoleAndTargetsToSyntheticSteps() {
        ResolvedStep step = resolvedStep("ProcessFolderService");
        Map<String, Object> config = aspectConfig("org.pipelineframework.plugin.persistence.PersistenceService");

        PipelineAspectModel aspect = new PipelineAspectModel(
            "persistence",
            AspectScope.GLOBAL,
            AspectPosition.BEFORE_STEP,
            config
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(
            List.of(step),
            List.of(aspect)
        );

        PipelineStepModel synthetic = expanded.stream()
            .map(ResolvedStep::model)
            .filter(PipelineStepModel::sideEffect)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected a synthetic side-effect step"));
        assertEquals(DeploymentRole.PLUGIN_SERVER, synthetic.deploymentRole());
        assertEquals(Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP), synthetic.enabledTargets());
        assertEquals(step.model().servicePackage(), synthetic.servicePackage());
        assertEquals("PersistenceService", synthetic.serviceClassName().simpleName());
        assertEquals(StreamingShape.UNARY_UNARY, synthetic.streamingShape());
    }

    @SneakyThrows
    @Test
    void syntheticStepsReuseGrpcBinding() {
        ResolvedStep step = resolvedStep("ProcessFolderService");

        Map<String, Object> config = aspectConfig("org.pipelineframework.plugin.persistence.PersistenceService");
        PipelineAspectModel aspect = new PipelineAspectModel(
            "persistence",
            AspectScope.GLOBAL,
            AspectPosition.AFTER_STEP,
            config
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(
            List.of(step),
            List.of(aspect)
        );

        ResolvedStep synthetic = expanded.get(1);
        assertNull(synthetic.grpcBinding(), "Synthetic step defers gRPC binding resolution");
        assertNotEquals(step.model().serviceName(), synthetic.model().serviceName());
        assertNotEquals(step.model().generatedName(), synthetic.model().generatedName());
    }

    @SneakyThrows
    @Test
    void expandsBeforeAndAfterAroundOriginalStep() {
        ResolvedStep step = resolvedStepWithInputOutput(
            "ProcessFolderService",
            "CsvFolder",
            "CsvPaymentsInputFile");

        PipelineAspectModel beforeAspect = new PipelineAspectModel(
            "cache-invalidate",
            AspectScope.GLOBAL,
            AspectPosition.BEFORE_STEP,
            aspectConfig("org.pipelineframework.plugin.cache.CacheInvalidationService")
        );
        PipelineAspectModel afterAspect = new PipelineAspectModel(
            "persistence",
            AspectScope.GLOBAL,
            AspectPosition.AFTER_STEP,
            aspectConfig("org.pipelineframework.plugin.persistence.PersistenceService")
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(
            List.of(step),
            List.of(beforeAspect, afterAspect)
        );

        assertEquals(3, expanded.size());
        assertEquals("ObserveCacheInvalidateCsvFolderSideEffectService", expanded.get(0).model().serviceName());
        assertEquals(step.model().serviceName(), expanded.get(1).model().serviceName());
        assertEquals("ObservePersistenceCsvPaymentsInputFileSideEffectService", expanded.get(2).model().serviceName());
    }

    @SneakyThrows
    @Test
    void normalizesHyphenatedAspectNameForGeneratedStep() {
        ResolvedStep step = resolvedStepWithInputOutput(
            "ProcessFolderService",
            "CsvFolder",
            "CsvPaymentsInputFile");

        PipelineAspectModel beforeAspect = new PipelineAspectModel(
            "cache-invalidate",
            AspectScope.GLOBAL,
            AspectPosition.BEFORE_STEP,
            aspectConfig("org.pipelineframework.plugin.cache.CacheInvalidationService")
        );

        AspectExpansionProcessor processor = new AspectExpansionProcessor();
        List<ResolvedStep> expanded = processor.expandAspects(
            List.of(step),
            List.of(beforeAspect)
        );

        assertEquals("CacheInvalidateCsvFolderSideEffect", expanded.get(0).model().generatedName());
        assertEquals("ObserveCacheInvalidateCsvFolderSideEffectService", expanded.get(0).model().serviceName());
    }

    private static ResolvedStep resolvedStep(String serviceName) throws Exception {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", serviceName))
            .inputMapping(new TypeMapping(ClassName.get("org.pipelineframework.csv.common.domain", "CsvFolder"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "CsvFolderMapper"), true))
            .outputMapping(new TypeMapping(ClassName.get("org.pipelineframework.csv.common.domain", "CsvPaymentsInputFile"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "CsvPaymentsInputFileMapper"), true))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .enabledTargets(Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .build();

        GrpcBindingResolver resolver = new GrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptors = loadDescriptorSet();
        GrpcBinding binding = resolver.resolve(model, descriptors);
        return new ResolvedStep(model, binding, null);
    }

    private static ResolvedStep resolvedStepWithoutBinding(String serviceName) {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", serviceName))
            .inputMapping(new TypeMapping(ClassName.get("org.pipelineframework.csv.common.domain", "CsvFolder"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "CsvFolderMapper"), true))
            .outputMapping(new TypeMapping(ClassName.get("org.pipelineframework.csv.common.domain", "CsvPaymentsInputFile"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "CsvPaymentsInputFileMapper"), true))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .enabledTargets(Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .build();
        return new ResolvedStep(model, null, null);
    }

    private static ResolvedStep resolvedStepWithInputOutput(String serviceName, String inputType, String outputType)
        throws Exception {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName(serviceName)
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get("org.pipelineframework.csv.service", serviceName))
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", inputType),
                ClassName.get("org.pipelineframework.csv.common.mapper", inputType + "Mapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", outputType),
                ClassName.get("org.pipelineframework.csv.common.mapper", outputType + "Mapper"),
                true))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .enabledTargets(Set.of(GenerationTarget.GRPC_SERVICE, GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .build();

        GrpcBindingResolver resolver = new GrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptors = loadDescriptorSet();
        GrpcBinding binding = resolver.resolve(model, descriptors);
        return new ResolvedStep(model, binding, null);
    }

    private static Map<String, Object> aspectConfig(String pluginImplementationClass) {
        Map<String, Object> config = new HashMap<>();
        config.put("pluginImplementationClass", pluginImplementationClass);
        return config;
    }

    private static DescriptorProtos.FileDescriptorSet loadDescriptorSet() throws IOException {
        try (InputStream stream = AspectExpansionProcessorTest.class.getResourceAsStream("/descriptor_set.dsc")) {
            if (stream == null) {
                throw new IOException("Missing test descriptor set resource: /descriptor_set.dsc");
            }
            return DescriptorProtos.FileDescriptorSet.parseFrom(stream);
        }
    }
}
