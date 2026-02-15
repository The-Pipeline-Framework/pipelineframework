package org.pipelineframework.processor.phase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;

import javax.annotation.processing.ProcessingEnvironment;

/**
 * Builds step-specific bindings (gRPC, REST, and local).
 */
class StepBindingBuilder {

    static final String GRPC_SUFFIX = "_grpc";
    static final String REST_SUFFIX = "_rest";
    static final String LOCAL_SUFFIX = "_local";
    static final String ORCHESTRATOR_KEY = "orchestrator";

    /**
     * Construct renderer-specific bindings for all step models.
     *
     * @param stepModels the pipeline step models
     * @param descriptorSet the protobuf descriptor set (may be null)
     * @param processingEnv the processing environment (may be null)
     * @return a map of bindings keyed by model name + suffix
     */
    static Map<String, Object> constructBindings(
            List<PipelineStepModel> stepModels,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            ProcessingEnvironment processingEnv) {
        Map<String, Object> bindingsMap = new HashMap<>();
        GrpcBindingResolver grpcBindingResolver = new GrpcBindingResolver();
        RestBindingResolver restBindingResolver = new RestBindingResolver();

        for (PipelineStepModel model : stepModels) {
            GrpcBinding grpcBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP)) {
                grpcBinding = grpcBindingResolver.resolve(model, descriptorSet);
            }

            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, processingEnv);
            }

            String modelKey = model.serviceName();
            if (grpcBinding != null) {
                bindingsMap.put(modelKey + GRPC_SUFFIX, grpcBinding);
            }
            if (restBinding != null) {
                bindingsMap.put(modelKey + REST_SUFFIX, restBinding);
            }
            if (model.enabledTargets().contains(GenerationTarget.LOCAL_CLIENT_STEP)) {
                bindingsMap.put(modelKey + LOCAL_SUFFIX, new LocalBinding(model));
            }
        }

        return bindingsMap;
    }

    /**
     * Rebuilds a gRPC binding with the specified model.
     *
     * @param binding the original binding (may be null)
     * @param model the pipeline step model
     * @return a new gRPC binding with the model applied
     */
    static GrpcBinding rebuildGrpcBinding(GrpcBinding binding, PipelineStepModel model) {
        if (binding == null) {
            return new GrpcBinding(model, null, null);
        }
        return new GrpcBinding(model, binding.serviceDescriptor(), binding.methodDescriptor());
    }

    /**
     * Rebuilds a REST binding with the specified model.
     *
     * @param binding the original binding (may be null)
     * @param model the pipeline step model
     * @return a new REST binding with the model applied
     */
    static RestBinding rebuildRestBinding(RestBinding binding, PipelineStepModel model) {
        if (binding == null) {
            return new RestBinding(model, null);
        }
        return new RestBinding(model, binding.restPathOverride());
    }
}
