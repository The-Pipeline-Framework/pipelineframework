package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.PipelineCompilationPhase;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.processor.util.DescriptorFileLocator;
import org.pipelineframework.processor.util.GrpcBindingResolver;
import org.pipelineframework.processor.util.RestBindingResolver;

/**
 * Constructs renderer-specific bindings from semantic models and resolved targets.
 * This phase reads semantic models and resolved targets, then constructs the appropriate
 * renderer-specific bindings and stores them in the compilation context.
 * <p>
 * Bindings are immutable and scoped to one renderer, never modifying semantic models.
 */
public class PipelineBindingConstructionPhase implements PipelineCompilationPhase {
    private final GrpcRequirementEvaluator grpcRequirementEvaluator;

    /**
     * Creates a PipelineBindingConstructionPhase initialized with a default GrpcRequirementEvaluator.
     */
    public PipelineBindingConstructionPhase() {
        this(new GrpcRequirementEvaluator());
    }

    /**
     * Construct a PipelineBindingConstructionPhase using the given gRPC requirement evaluator.
     *
     * @param grpcRequirementEvaluator evaluator used to decide whether gRPC descriptor loading and binding resolution are required
     * @throws NullPointerException if {@code grpcRequirementEvaluator} is null
     */
    public PipelineBindingConstructionPhase(GrpcRequirementEvaluator grpcRequirementEvaluator) {
        this.grpcRequirementEvaluator = Objects.requireNonNull(grpcRequirementEvaluator, "grpcRequirementEvaluator");
    }

    /**
     * Identifier for the pipeline binding construction phase.
     *
     * @return the phase name "Pipeline Binding Construction Phase"
     */
    @Override
    public String name() {
        return "Pipeline Binding Construction Phase";
    }

    /**
     * Constructs renderer-specific bindings for each pipeline step and stores them in the compilation context.
     *
     * <p>This builds gRPC, REST and local bindings as appropriate for each PipelineStepModel, optionally
     * loads a protocol descriptor set when gRPC bindings are required, and adds an orchestrator binding
     * if orchestrator models are present. Bindings are stored in the context's renderer bindings map
     * using keys: {@code <modelName>_grpc}, {@code <modelName>_rest},
     * {@code <modelName>_local} and {@code orchestrator}.
     *
     * @param ctx the pipeline compilation context to read models from and write constructed bindings into
     * @throws Exception if descriptor set loading or binding construction fails
     */
    @Override
    public void execute(PipelineCompilationContext ctx) throws Exception {
        // Create a map to store bindings for each model
        Map<String, Object> bindingsMap = new HashMap<>();

        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.getDescriptorSet();

        if (descriptorSet == null && needsGrpcBindings(ctx)) {
            descriptorSet = loadDescriptorSet(ctx);
            ctx.setDescriptorSet(descriptorSet);
        }

        GrpcBindingResolver grpcBindingResolver = new GrpcBindingResolver();
        RestBindingResolver restBindingResolver = new RestBindingResolver();
        
        // Process each step model to construct appropriate bindings
        for (PipelineStepModel model : ctx.getStepModels()) {
            String modelKey = model.serviceName();

            // Delegated steps also need external adapters in addition to regular client bindings.
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

            // Construct gRPC binding if needed
            GrpcBinding grpcBinding = null;
            if (model.delegateService() == null
                && (model.enabledTargets().contains(GenerationTarget.GRPC_SERVICE)
                || model.enabledTargets().contains(GenerationTarget.CLIENT_STEP))) {
                grpcBinding = grpcBindingResolver.resolve(model, descriptorSet);
            }

            // Construct REST binding if needed
            RestBinding restBinding = null;
            if (model.enabledTargets().contains(GenerationTarget.REST_RESOURCE)
                || model.enabledTargets().contains(GenerationTarget.REST_CLIENT_STEP)) {
                restBinding = restBindingResolver.resolve(model, ctx.getProcessingEnv());
            }

            // Store bindings for this model conditionally
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
        
        // Process orchestrator models if present
        if (!ctx.getOrchestratorModels().isEmpty()) {
            OrchestratorBinding orchestratorBinding = OrchestratorBindingBuilder.buildOrchestratorBinding(
                (PipelineTemplateConfig) ctx.getPipelineTemplateConfig(),
                ctx.getRoundEnv() != null ? ctx.getRoundEnv().getElementsAnnotatedWith(PipelineOrchestrator.class) : Set.of()
            );
            if (orchestratorBinding != null) {
                bindingsMap.put("orchestrator", orchestratorBinding);
            }
        }
        
        // Store the bindings map in the context
        ctx.setRendererBindings(bindingsMap);
    }

    /**
     * Determine whether the compilation context requires gRPC bindings.
     *
     * Considers step models, orchestrator models, and the pipeline template configuration to decide
     * if descriptor loading and gRPC binding resolution are needed.
     *
     * @param ctx the pipeline compilation context to inspect for models and configuration
     * @return `true` if gRPC bindings are required, `false` otherwise
     */
    private boolean needsGrpcBindings(PipelineCompilationContext ctx) {
        PipelineTemplateConfig config = ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig cfg ? cfg : null;
        return grpcRequirementEvaluator.needsGrpcBindings(
            ctx.getStepModels(),
            ctx.getOrchestratorModels(),
            config,
            ctx.getProcessingEnv() != null ? ctx.getProcessingEnv().getMessager() : null
        );
    }

    /**
     * Load the protobuf DescriptorProtos.FileDescriptorSet for pipeline step models that require gRPC descriptors.
     *
     * @param ctx the compilation context providing step models and the processing environment
     * @return the descriptor set containing descriptors for non-delegated step services
     * @throws IOException if descriptor files cannot be located or read
     */
    private DescriptorProtos.FileDescriptorSet loadDescriptorSet(PipelineCompilationContext ctx) throws IOException {
        DescriptorFileLocator locator = new DescriptorFileLocator();
        Set<String> expectedServices = ctx.getStepModels().stream()
            .filter(model -> model.delegateService() == null)
            .map(PipelineStepModel::serviceName)
            .collect(Collectors.toSet());
        return locator.locateAndLoadDescriptors(
            ctx.getProcessingEnv().getOptions(),
            expectedServices,
            ctx.getProcessingEnv().getMessager());
    }

    /**
     * Warns when a delegated pipeline step declares server targets that will be ignored.
     *
     * <p>If the processing environment or its {@code Messager} is not available, no action is taken.
     *
     * @param ctx   the compilation context providing the processing environment and diagnostics
     * @param model the delegated step model to inspect for server targets
     */
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