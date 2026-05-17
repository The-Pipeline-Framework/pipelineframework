package org.pipelineframework.processor.util;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;

/**
 * Resolves generated service types used by transport adapters.
 */
public final class GeneratedServiceTypeResolver {

    private GeneratedServiceTypeResolver() {
    }

    /**
     * Resolves the service type that transport adapters should inject for a pipeline step model.
     *
     * @param model the pipeline step model to inspect; must not be {@code null}
     * @return the authored service type, generated side-effect bean type, or generated blocking
     *         reactive bridge type depending on {@code sideEffect()} and {@code serviceApiKind()}
     * @throws IllegalArgumentException if {@code model} is {@code null}
     * @throws IllegalStateException if the resolved default service path requires
     *         {@code serviceClassName()} but it is {@code null}
     */
    public static TypeName resolveInjectedServiceType(PipelineStepModel model) {
        if (model == null) {
            throw new IllegalArgumentException("model cannot be null");
        }
        if (model.sideEffect()) {
            return ClassName.get(
                model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
                model.serviceName());
        }
        boolean useBlockingBridge = (model.serviceApiKind() == ServiceApiKind.BLOCKING
            || model.serviceApiKind() == ServiceApiKind.BLOCKING_ITERATOR)
            && model.delegateService() == null
            && (model.remoteExecution() == null || !model.remoteExecution().isRemote());
        if (useBlockingBridge) {
            return blockingReactiveBridgeClassName(model);
        }
        if (model.serviceClassName() == null) {
            throw new IllegalStateException("serviceClassName cannot be null for service injection");
        }
        return model.serviceClassName();
    }

    public static ClassName blockingReactiveBridgeClassName(PipelineStepModel model) {
        return ClassName.get(
            model.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX,
            model.generatedName() + "BlockingReactiveBridge");
    }
}
