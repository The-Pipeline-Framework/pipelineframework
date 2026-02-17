package org.pipelineframework.processor.phase;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.ExternalAdapterBinding;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;

/**
 * Builds per-step renderer bindings for the binding construction phase.
 */
class StepBindingConstructionService {

    private final GrpcBindingResolver grpcBindingResolver;
    private final RestBindingResolver restBindingResolver;

    StepBindingConstructionService() {
        this(new GrpcBindingResolver(), new RestBindingResolver());
    }

    StepBindingConstructionService(GrpcBindingResolver grpcBindingResolver, RestBindingResolver restBindingResolver) {
        this.grpcBindingResolver = Objects.requireNonNull(grpcBindingResolver, "grpcBindingResolver");
        this.restBindingResolver = Objects.requireNonNull(restBindingResolver, "restBindingResolver");
    }

    Map<String, Object> buildBindings(PipelineCompilationContext ctx, DescriptorProtos.FileDescriptorSet descriptorSet) {
        Map<String, Object> bindingsMap = new HashMap<>();
        for (PipelineStepModel model : ctx.getStepModels()) {
            String modelKey = model.serviceName();

            if (model.delegateService() != null) {
                warnIfDelegatedStepHasServerTargets(ctx, model);
                ExternalAdapterBinding externalAdapterBinding = new ExternalAdapterBinding(
                    model,
                    model.serviceName(),
                    model.servicePackage(),
                    model.delegateService().toString(),
                    model.externalMapper() != null ? model.externalMapper().toString() : null
                );
                bindingsMap.put(modelKey + "_external_adapter", externalAdapterBinding);
            }

            GrpcBinding grpcBinding = null;
            if (model.delegateService() == null
                && (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP))) {
                grpcBinding = grpcBindingResolver.resolve(model, descriptorSet);
            }

            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, ctx.getProcessingEnv());
            }

            if (grpcBinding != null) {
                bindingsMap.put(modelKey + "_grpc", grpcBinding);
            }
            if (restBinding != null) {
                bindingsMap.put(modelKey + "_rest", restBinding);
            }
            if (model.enabledTargets().contains(GenerationTarget.LOCAL_CLIENT_STEP)) {
                bindingsMap.put(modelKey + "_local", new LocalBinding(model));
            }
        }
        return bindingsMap;
    }

    private void warnIfDelegatedStepHasServerTargets(PipelineCompilationContext ctx, PipelineStepModel model) {
        if (ctx.getProcessingEnv() == null || ctx.getProcessingEnv().getMessager() == null) {
            return;
        }
        Set<GenerationTarget> ignoredTargets = model.enabledTargets().stream()
            .filter(target -> target == GenerationTarget.GRPC_SERVICE || target == GenerationTarget.REST_RESOURCE)
            .collect(Collectors.toSet());
        if (ignoredTargets.isEmpty()) {
            return;
        }

        String ignoredTargetsMessage = ignoredTargets.stream().map(Enum::name).sorted().collect(Collectors.joining(", "));
        ctx.getProcessingEnv().getMessager().printMessage(
            javax.tools.Diagnostic.Kind.WARNING,
            "Delegated step '" + model.serviceName() + "' ignores server targets ["
                + ignoredTargetsMessage
                + "]. Delegated steps generate external adapters plus client bindings.");
    }
}
