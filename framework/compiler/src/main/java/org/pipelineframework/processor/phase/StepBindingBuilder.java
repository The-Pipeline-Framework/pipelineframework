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
    
    private static final GrpcBindingResolver GRPC_RESOLVER = new GrpcBindingResolver();
    private static final RestBindingResolver REST_RESOLVER = new RestBindingResolver();

    /**
     * Construct renderer-specific bindings for all step models.
     * 
     * This method returns a Map<String, Object> that accommodates heterogeneous binding types
     * (GrpcBinding, RestBinding, LocalBinding) for Mustache/template engines and other templating
     * libraries. Callers will need to cast entries to the concrete types when accessing them.
     * 
     * @param stepModels the pipeline step models
     * @param descriptorSet the protobuf descriptor set (may be null)
     * @param processingEnv the processing environment (may be null)
     * @return a map of bindings keyed by model name + suffix; values are of type Object and must be cast to concrete binding types
     * @throws IllegalStateException if duplicate service names are detected that would cause silent overwrites
     */
    static Map<String, Object> constructBindings(
            List<PipelineStepModel> stepModels,
            DescriptorProtos.FileDescriptorSet descriptorSet,
            ProcessingEnvironment processingEnv) {
        Map<String, Object> bindingsMap = new HashMap<>();

        for (PipelineStepModel model : stepModels) {
            GrpcBinding grpcBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP)) {
                grpcBinding = GRPC_RESOLVER.resolve(model, descriptorSet);
            }

            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = REST_RESOLVER.resolve(model, processingEnv);
            }

            String modelKey = model.serviceName();
            
            // Check for duplicate keys before inserting to prevent silent overwrites
            if (grpcBinding != null) {
                String grpcKey = modelKey + GRPC_SUFFIX;
                if (bindingsMap.containsKey(grpcKey)) {
                    throw new IllegalStateException("Duplicate service name detected: " + modelKey +
                        " for GRPC binding. Service names must be unique. Generated name: " + model.generatedName());
                }
                bindingsMap.put(grpcKey, grpcBinding);
            }
            if (restBinding != null) {
                String restKey = modelKey + REST_SUFFIX;
                if (bindingsMap.containsKey(restKey)) {
                    throw new IllegalStateException("Duplicate service name detected: " + modelKey +
                        " for REST binding. Service names must be unique. Generated name: " + model.generatedName());
                }
                bindingsMap.put(restKey, restBinding);
            }
            if (model.enabledTargets().contains(GenerationTarget.LOCAL_CLIENT_STEP)) {
                String localKey = modelKey + LOCAL_SUFFIX;
                if (bindingsMap.containsKey(localKey)) {
                    throw new IllegalStateException("Duplicate service name detected: " + modelKey +
                        " for LOCAL binding. Service names must be unique. Generated name: " + model.generatedName());
                }
                bindingsMap.put(localKey, new LocalBinding(model));
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
