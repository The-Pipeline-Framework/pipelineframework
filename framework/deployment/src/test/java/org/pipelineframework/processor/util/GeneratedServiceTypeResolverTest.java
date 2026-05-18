package org.pipelineframework.processor.util;

import java.util.Set;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateRemoteTarget;
import org.pipelineframework.config.template.PipelineTemplateStepExecution;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link GeneratedServiceTypeResolver}.
 */
class GeneratedServiceTypeResolverTest {

    private static final String SERVICE_PACKAGE = "com.example.service";
    private static final String SERVICE_NAME = "MyService";
    private static final String GENERATED_NAME = "MyServiceGenerated";
    private static final ClassName SERVICE_CLASS_NAME = ClassName.get(SERVICE_PACKAGE, SERVICE_NAME);

    @Test
    void resolveInjectedServiceType_throwsWhenModelIsNull() {
        assertThrows(IllegalArgumentException.class,
            () -> GeneratedServiceTypeResolver.resolveInjectedServiceType(null));
    }

    @Test
    void resolveInjectedServiceType_sideEffectReturnsGeneratedBeanInPipelinePackage() {
        PipelineStepModel model = baseBuilder()
            .sideEffect(true)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        ClassName expected = ClassName.get(
            SERVICE_PACKAGE + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            SERVICE_NAME);
        assertEquals(expected, result);
    }

    @Test
    void resolveInjectedServiceType_blockingWithNoDelegateAndNoRemoteReturnsReactiveBridgeType() {
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        assertEquals(GeneratedServiceTypeResolver.blockingReactiveBridgeClassName(model), result);
    }

    @Test
    void resolveInjectedServiceType_blockingIteratorWithNoDelegateAndNoRemoteReturnsReactiveBridgeType() {
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.BLOCKING_ITERATOR)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        assertEquals(GeneratedServiceTypeResolver.blockingReactiveBridgeClassName(model), result);
    }

    @Test
    void resolveInjectedServiceType_reactiveReturnsServiceClassName() {
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.REACTIVE)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        assertEquals(SERVICE_CLASS_NAME, result);
    }

    @Test
    void resolveInjectedServiceType_blockingWithDelegateReturnsServiceClassName() {
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .delegateService(ClassName.get("com.external.lib", "ExternalService"))
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        assertEquals(SERVICE_CLASS_NAME, result);
    }

    @Test
    void resolveInjectedServiceType_blockingWithRemoteExecutionReturnsServiceClassName() {
        PipelineTemplateStepExecution remoteExecution = new PipelineTemplateStepExecution(
            "REMOTE", "my-operator", "PROTOBUF_HTTP_V1", 3000,
            new PipelineTemplateRemoteTarget("https://operator.example/process", null));
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .remoteExecution(remoteExecution)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        assertEquals(SERVICE_CLASS_NAME, result);
    }

    @Test
    void resolveInjectedServiceType_blockingWithLocalExecutionReturnsReactiveBridgeType() {
        PipelineTemplateStepExecution localExecution = new PipelineTemplateStepExecution(
            "LOCAL", null, null, null, null);
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .remoteExecution(localExecution)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        assertEquals(GeneratedServiceTypeResolver.blockingReactiveBridgeClassName(model), result);
    }

    @Test
    void resolveInjectedServiceType_serviceClassNameIsCheckedByConstructor() {
        // PipelineStepModel enforces non-null serviceClassName at construction time.
        // Verify that the constructor guard prevents a null serviceClassName model
        // from even being created, ensuring the resolver never encounters it.
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineStepModel(
                SERVICE_NAME,
                GENERATED_NAME,
                SERVICE_PACKAGE,
                null, // serviceClassName is null — constructor rejects this
                new TypeMapping(TypeName.INT, null, false),
                new TypeMapping(TypeName.INT, null, false),
                StreamingShape.UNARY_UNARY,
                Set.of(GenerationTarget.GRPC_SERVICE),
                ExecutionMode.DEFAULT,
                DeploymentRole.PIPELINE_SERVER,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                ServiceApiKind.REACTIVE));
    }

    @Test
    void blockingReactiveBridgeClassName_returnsCorrectNameInPipelinePackage() {
        PipelineStepModel model = baseBuilder()
            .serviceApiKind(ServiceApiKind.BLOCKING_ITERATOR)
            .build();

        ClassName result = GeneratedServiceTypeResolver.blockingReactiveBridgeClassName(model);

        assertEquals(
            SERVICE_PACKAGE + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            result.packageName());
        assertEquals(GENERATED_NAME + "BlockingReactiveBridge", result.simpleName());
    }

    @Test
    void blockingReactiveBridgeClassName_usesGeneratedNameNotServiceName() {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ActualServiceName")
            .generatedName("GeneratedBridgeName")
            .servicePackage(SERVICE_PACKAGE)
            .serviceClassName(ClassName.get(SERVICE_PACKAGE, "ActualServiceName"))
            .inputMapping(new TypeMapping(TypeName.INT, null, false))
            .outputMapping(new TypeMapping(TypeName.INT, null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of())
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .build();

        ClassName result = GeneratedServiceTypeResolver.blockingReactiveBridgeClassName(model);

        assertEquals("GeneratedBridgeName" + "BlockingReactiveBridge", result.simpleName());
    }

    @Test
    void resolveInjectedServiceType_sideEffectTakesPrecedenceOverBlocking() {
        // sideEffect=true + BLOCKING → should return side-effect bean, not bridge
        PipelineStepModel model = baseBuilder()
            .sideEffect(true)
            .serviceApiKind(ServiceApiKind.BLOCKING)
            .build();

        TypeName result = GeneratedServiceTypeResolver.resolveInjectedServiceType(model);

        ClassName expected = ClassName.get(
            SERVICE_PACKAGE + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            SERVICE_NAME);
        assertEquals(expected, result);
    }

    private PipelineStepModel.Builder baseBuilder() {
        return new PipelineStepModel.Builder()
            .serviceName(SERVICE_NAME)
            .generatedName(GENERATED_NAME)
            .servicePackage(SERVICE_PACKAGE)
            .serviceClassName(SERVICE_CLASS_NAME)
            .inputMapping(new TypeMapping(ClassName.get("com.example.domain", "Input"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.domain", "Output"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.GRPC_SERVICE))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER);
    }
}
