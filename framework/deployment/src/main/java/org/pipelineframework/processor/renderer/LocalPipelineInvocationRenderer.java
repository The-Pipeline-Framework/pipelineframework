package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.Modifier;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.quarkus.arc.Unremovable;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.composition.CompiledPipelineLocation;
import org.pipelineframework.processor.composition.DefinitionLocalLocation;
import org.pipelineframework.processor.composition.PipelineDefinition;
import org.pipelineframework.processor.composition.PipelineDefinitionStep;
import org.pipelineframework.processor.composition.PipelineInvocationBinding;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.composition.ResolvedPipelineDefinitionGraph;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.util.ClientStepClassNames;

/** Generates the local CDI realization selected from compiler-owned invocation bindings. */
public final class LocalPipelineInvocationRenderer {
    private static final ClassName PIPELINE_RUNNER = ClassName.get(PipelineRunner.class);
    private static final ClassName INVOCATION_STEPS = ClassName.get("org.pipelineframework.invocation", "PipelineInvocationSteps");
    private static final ClassName CONFIGURABLE_STEP = ClassName.get("org.pipelineframework.step", "ConfigurableStep");

    public void render(PipelineCompilationContext ctx, Path outputDir) throws IOException {
        var resolvedGraph = ctx.getResolvedPipelineDefinitionGraph();
        if (resolvedGraph.isEmpty() || !(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig template)) {
            return;
        }
        ResolvedPipelineDefinitionGraph graph = resolvedGraph.orElseThrow();
        Map<CompiledPipelineLocation, PipelineInvocationBinding> bindings = new LinkedHashMap<>();
        for (PipelineInvocationBinding binding : graph.invocationBindings()) {
            bindings.put(binding.invocationLocation(), binding);
        }
        Map<CompiledPipelineLocation, ClassName> invocationTypes = new LinkedHashMap<>();
        for (PipelineInvocationBinding binding : bindings.values()) {
            invocationTypes.put(binding.invocationLocation(), ClassName.get(template.basePackage() + ".pipeline",
                invocationClassName(binding.invocationLocation())));
        }
        for (PipelineInvocationBinding binding : bindings.values()) {
            writeInvocation(ctx, outputDir, graph, binding, invocationTypes);
        }
        ctx.setGeneratedRootPipelineStepClasses(rootClasses(ctx, bindings, invocationTypes));
    }

    private void writeInvocation(PipelineCompilationContext ctx, Path outputDir, ResolvedPipelineDefinitionGraph graph,
            PipelineInvocationBinding binding,
            Map<CompiledPipelineLocation, ClassName> invocationTypes) throws IOException {
        PipelineDefinition target = graph.definitions().get(binding.target());
        StepDefinition callsite = invocationCallsite(ctx, binding);
        TypeName input = callsite.inputType();
        TypeName output = callsite.outputType();
        TypeSpec.Builder type = TypeSpec.classBuilder(invocationTypes.get(binding.invocationLocation()).simpleName())
            .addModifiers(Modifier.PUBLIC)
            .superclass(CONFIGURABLE_STEP)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "Dependent"))
            .addAnnotation(ClassName.get(Unremovable.class))
            .addField(FieldSpec.builder(PIPELINE_RUNNER, "runner")
                .addAnnotation(ClassName.get("jakarta.inject", "Inject")).build());
        ClassName iface = interfaceFor(binding.cardinality());
        type.addSuperinterface(ParameterizedTypeName.get(iface, input, output));
        List<String> childFields = new ArrayList<>();
        for (PipelineDefinitionStep step : target.steps()) {
            ClassName childType = childType(ctx, binding, step, invocationTypes);
            String field = "child" + childFields.size();
            type.addField(FieldSpec.builder(childType, field)
                .addAnnotation(ClassName.get("jakarta.inject", "Inject")).build());
            childFields.add(field);
        }
        var targetPlan = ctx.getLocalDefinitionBranchingPlans().get(binding.target());
        if (targetPlan == null) {
            throw new IllegalStateException("No branch plan for linked pipeline definition '"
                + binding.target().logicalId() + "'.");
        }
        type.addMethod(invocationMethod(
            binding.cardinality(), input, output, binding.target().logicalId(), targetPlan.terminalStepIndex(), childFields));
        JavaFile.builder(invocationTypes.get(binding.invocationLocation()).packageName(), type.build()).build().writeTo(outputDir);
    }

    private ClassName childType(PipelineCompilationContext ctx, PipelineInvocationBinding parent,
            PipelineDefinitionStep child, Map<CompiledPipelineLocation, ClassName> invocationTypes) {
        if (child.pipelineReference().isPresent()) {
            List<DefinitionLocalLocation> path = new ArrayList<>(parent.invocationLocation().invocationPath());
            path.add(parent.invocationLocation().definitionLocalLocation());
            CompiledPipelineLocation location = new CompiledPipelineLocation(path,
                new DefinitionLocalLocation(parent.target(), child.localStepId()));
            ClassName type = invocationTypes.get(location);
            if (type == null) {
                throw new IllegalStateException("No generated invocation type for " + location.display());
            }
            return type;
        }
        PipelineStepModel model = ctx.getLocalDefinitionStepModels().getOrDefault(parent.target().logicalId(), List.of()).stream()
            .filter(candidate -> candidate.serviceName().equals(toYamlServiceName(child.localStepId())))
            .findFirst().orElseThrow(() -> new IllegalStateException("No generated child step model for '"
                + child.localStepId() + "' in definition '" + parent.target().logicalId() + "'."));
        return ClassName.bestGuess(ClientStepClassNames.className(model, ctx.getTransportMode()));
    }

    private List<String> rootClasses(PipelineCompilationContext ctx,
            Map<CompiledPipelineLocation, PipelineInvocationBinding> bindings,
            Map<CompiledPipelineLocation, ClassName> invocationTypes) {
        List<String> classes = new ArrayList<>();
        for (StepDefinition root : ctx.getStepDefinitions()) {
            if (root.kind() == org.pipelineframework.processor.ir.StepKind.PIPELINE) {
                CompiledPipelineLocation location = new CompiledPipelineLocation(List.of(),
                    new DefinitionLocalLocation(new PipelineReference("$root"), root.name()));
                PipelineInvocationBinding binding = bindings.get(location);
                if (binding == null) {
                    throw new IllegalStateException("No resolved pipeline binding for root step '" + root.name() + "'.");
                }
                classes.add(invocationTypes.get(binding.invocationLocation()).canonicalName());
                continue;
            }
            PipelineStepModel model = ctx.getStepModels().stream()
                .filter(candidate -> candidate.serviceName().equals(toYamlServiceName(root.name())))
                .findFirst().orElseThrow(() -> new IllegalStateException("No generated root step model for '" + root.name() + "'."));
            if (hasGeneratedClient(model)) {
                classes.add(ClientStepClassNames.className(model, ctx.getTransportMode()));
            } else if (model.serviceClassName() != null) {
                classes.add(model.serviceClassName().canonicalName());
            } else {
                throw new IllegalStateException("No local runtime class for root step '" + root.name() + "'.");
            }
        }
        return List.copyOf(classes);
    }

    private boolean hasGeneratedClient(PipelineStepModel model) {
        return model.enabledTargets().contains(GenerationTarget.LOCAL_CLIENT_STEP)
            || model.enabledTargets().contains(GenerationTarget.AWAIT_CLIENT_STEP)
            || model.enabledTargets().contains(GenerationTarget.COMMAND_CLIENT_STEP)
            || model.enabledTargets().contains(GenerationTarget.QUERY_CLIENT_STEP);
    }

    private StepDefinition invocationCallsite(PipelineCompilationContext ctx, PipelineInvocationBinding binding) {
        DefinitionLocalLocation location = binding.invocationLocation().definitionLocalLocation();
        List<StepDefinition> ownerSteps = "$root".equals(location.definition().logicalId())
            ? ctx.getParsedPipelineDefinitionCatalog().rootSteps()
            : ctx.getParsedPipelineDefinitionCatalog().localDefinitions().get(location.definition().logicalId());
        if (ownerSteps == null) {
            throw new IllegalStateException("No parsed owner definition for " + location.definition().logicalId());
        }
        return ownerSteps.stream()
            .filter(step -> step.name().equals(location.localStepId()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No parsed pipeline invocation callsite for "
                + binding.invocationLocation().display()));
    }

    private MethodSpec invocationMethod(CardinalitySemantics cardinality, TypeName input, TypeName output,
            String definitionId, int definitionTerminalStepIndex, List<String> children) {
        String list = "java.util.List.of(" + String.join(", ", children) + ")";
        return switch (cardinality) {
            case ONE_TO_ONE -> MethodSpec.methodBuilder("applyOneToOne").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get("io.smallrye.mutiny", "Uni"), output)).addParameter(input, "input")
                .addStatement("return $T.<$T, $T>oneToOne(runner, $S, $L, $L).applyOneToOne(input)",
                    INVOCATION_STEPS, input, output, definitionId, definitionTerminalStepIndex, list).build();
            case ONE_TO_MANY -> MethodSpec.methodBuilder("applyOneToMany").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get("io.smallrye.mutiny", "Multi"), output)).addParameter(input, "input")
                .addStatement("return $T.<$T, $T>oneToMany(runner, $S, $L, $L).applyOneToMany(input)",
                    INVOCATION_STEPS, input, output, definitionId, definitionTerminalStepIndex, list).build();
            case MANY_TO_ONE -> MethodSpec.methodBuilder("apply").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get("io.smallrye.mutiny", "Uni"), output))
                .addParameter(ParameterizedTypeName.get(ClassName.get("io.smallrye.mutiny", "Multi"), input), "input")
                .addStatement("return $T.<$T, $T>manyToOne(runner, $S, $L, $L).apply(input)",
                    INVOCATION_STEPS, input, output, definitionId, definitionTerminalStepIndex, list).build();
            case MANY_TO_MANY -> MethodSpec.methodBuilder("applyTransform").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get("io.smallrye.mutiny", "Multi"), output))
                .addParameter(ParameterizedTypeName.get(ClassName.get("io.smallrye.mutiny", "Multi"), input), "input")
                .addStatement("return $T.<$T, $T>manyToMany(runner, $S, $L, $L).applyTransform(input)",
                    INVOCATION_STEPS, input, output, definitionId, definitionTerminalStepIndex, list).build();
        };
    }

    private ClassName interfaceFor(CardinalitySemantics cardinality) {
        return switch (cardinality) {
            case ONE_TO_ONE -> ClassName.get("org.pipelineframework.step", "StepOneToOne");
            case ONE_TO_MANY -> ClassName.get("org.pipelineframework.step", "StepOneToMany");
            case MANY_TO_ONE -> ClassName.get("org.pipelineframework.step.functional", "ManyToOne");
            case MANY_TO_MANY -> ClassName.get("org.pipelineframework.step", "StepManyToMany");
        };
    }

    private String invocationClassName(CompiledPipelineLocation location) {
        String normalized = location.display().replaceAll("[^A-Za-z0-9_]", "_")
            .replaceAll("_+", "_").replaceFirst("^_+", "");
        return "PipelineInvocation_" + normalized;
    }

    private static String toYamlServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String stripped = stepName.replaceFirst("(?i)^process\\s+", "");
        String formatted = java.util.Arrays.stream(stripped.split("[^A-Za-z0-9]+"))
            .filter(part -> !part.isBlank())
            .map(part -> Character.toUpperCase(part.charAt(0)) + part.substring(1))
            .collect(java.util.stream.Collectors.joining());
        return formatted == null || formatted.isBlank() ? "ProcessStepService" : "Process" + formatted + "Service";
    }
}
