package org.pipelineframework.processor.routing;

import java.util.*;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.config.template.PipelineTemplateUnion;
import org.pipelineframework.config.template.PipelineTemplateUnionVariant;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.StepDefinition;

/**
 * Validates and plans linear union-routed branching.
 */
public final class PipelineBranchRoutingPlanner {

    /**
     * Build a branching plan for the current compilation context.
     *
     * @param ctx compilation context
     * @return Optional containing the plan when successful, or empty after reporting diagnostics on validation failure
     */
    public Optional<PipelineBranchingPlan> plan(PipelineCompilationContext ctx) {
        if (ctx == null) {
            return Optional.of(PipelineBranchingPlan.disabled());
        }
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig templateConfig)) {
            return planWithoutTemplate(ctx);
        }
        List<PipelineTemplateStep> templateSteps = templateConfig.steps() == null ? List.of() : templateConfig.steps();
        if (templateSteps.isEmpty()) {
            return Optional.of(PipelineBranchingPlan.disabled());
        }

        Map<String, PipelineTemplateUnion> unions = templateConfig.unions();
        boolean branchAware = templateSteps.stream()
            .filter(Objects::nonNull)
            .anyMatch(step -> !step.accepts().isEmpty() || step.terminal()
                || (step.inputTypeName() != null && unions.containsKey(step.inputTypeName())));
        if (!branchAware) {
            return Optional.of(PipelineBranchingPlan.disabled());
        }

        if (templateConfig.version() < 2) {
            error(ctx, "Branch-aware routing requires version: 2 pipeline templates.");
            return Optional.empty();
        }

        Optional<Map<String, StepDefinition>> definitionsByName = indexStepDefinitions(ctx, templateSteps);
        if (definitionsByName.isEmpty()) {
            return Optional.empty();
        }
        Map<String, ClassName> contractRuntimeTypes = indexContractRuntimeTypes(
            ctx,
            templateConfig,
            templateSteps,
            definitionsByName.orElseThrow());

        List<ResolvedStep> resolvedSteps = new ArrayList<>();
        boolean valid = true;
        int terminalCount = 0;
        int terminalIndex = -1;
        for (int index = 0; index < templateSteps.size(); index++) {
            PipelineTemplateStep templateStep = templateSteps.get(index);
            if (templateStep == null) {
                continue;
            }
            StepDefinition stepDefinition = definitionsByName.orElseThrow().get(normalizeStepName(templateStep.name()));
            if (stepDefinition == null) {
                error(ctx, "Branch-aware step '" + templateStep.name()
                    + "' was not resolved into a step definition. Fix existing YAML or parser errors first.");
                valid = false;
                continue;
            }
            Optional<ResolvedStep> resolved = resolveStep(
                ctx,
                templateConfig,
                templateStep,
                stepDefinition,
                index,
                contractRuntimeTypes);
            if (resolved.isEmpty()) {
                valid = false;
                continue;
            }
            resolvedSteps.add(resolved.orElseThrow());
            if (resolved.orElseThrow().terminal()) {
                terminalCount++;
                terminalIndex = index;
            }
        }
        if (!valid) {
            return Optional.empty();
        }

        if (terminalCount == 0) {
            error(ctx, "Branch-aware pipelines require exactly one step with terminal: true.");
            return Optional.empty();
        }
        if (terminalCount > 1) {
            error(ctx, "Branch-aware pipelines may declare only one terminal: true step.");
            return Optional.empty();
        }
        if (terminalIndex != resolvedSteps.size() - 1) {
            error(ctx, "The terminal: true step must be the last authored step in a branch-aware pipeline.");
            return Optional.empty();
        }

        if (!validateReachability(ctx, resolvedSteps)) {
            return Optional.empty();
        }

        List<PipelineBranchingPlan.BranchStep> steps = resolvedSteps.stream()
            .map(step -> new PipelineBranchingPlan.BranchStep(
                step.index(),
                step.templateStep().name(),
                step.templateStep().inputTypeName(),
                step.templateStep().outputTypeName(),
                List.copyOf(step.acceptedLeafTypes()),
                List.copyOf(step.producedLeafTypes()),
                List.copyOf(step.acceptedDomainTypes()),
                step.terminal()))
            .toList();
        return Optional.of(new PipelineBranchingPlan(true, terminalIndex, steps));
    }

    private Optional<PipelineBranchingPlan> planWithoutTemplate(PipelineCompilationContext ctx) {
        List<StepDefinition> stepDefinitions = ctx.getStepDefinitions() == null ? List.of() : ctx.getStepDefinitions();
        boolean branchAware = stepDefinitions.stream()
            .filter(Objects::nonNull)
            .anyMatch(step -> !step.accepts().isEmpty() || step.terminal());
        if (!branchAware) {
            return Optional.of(PipelineBranchingPlan.disabled());
        }
        error(ctx, "Branch-aware routing requires template contract metadata. Add version: 2 messages/unions and typed step declarations.");
        return Optional.empty();
    }

    private Optional<Map<String, StepDefinition>> indexStepDefinitions(
        PipelineCompilationContext ctx,
        List<PipelineTemplateStep> templateSteps
    ) {
        List<StepDefinition> stepDefinitions = ctx.getStepDefinitions() == null ? List.of() : ctx.getStepDefinitions();
        Map<String, StepDefinition> indexed = new LinkedHashMap<>();
        Set<String> duplicateNames = new LinkedHashSet<>();
        for (StepDefinition stepDefinition : stepDefinitions) {
            if (stepDefinition == null || stepDefinition.name() == null) {
                continue;
            }
            String key = normalizeStepName(stepDefinition.name());
            StepDefinition previous = indexed.putIfAbsent(key, stepDefinition);
            if (previous != null) {
                duplicateNames.add(stepDefinition.name());
            }
        }
        if (!duplicateNames.isEmpty()) {
            error(ctx, "Branch-aware pipelines require unique step names. Duplicate names: " + duplicateNames);
            return Optional.empty();
        }

        boolean valid = true;
        for (PipelineTemplateStep templateStep : templateSteps) {
            if (templateStep == null) {
                continue;
            }
            if (!indexed.containsKey(normalizeStepName(templateStep.name()))) {
                error(ctx, "Step definition missing for authored step '" + templateStep.name() + "'.");
                valid = false;
            }
        }
        return valid ? Optional.of(indexed) : Optional.empty();
    }

    private Optional<ResolvedStep> resolveStep(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig templateConfig,
        PipelineTemplateStep templateStep,
        StepDefinition stepDefinition,
        int index,
        Map<String, ClassName> contractRuntimeTypes
    ) {
        if (isBlank(templateStep.inputTypeName())) {
            error(ctx, "Branch-aware step '" + templateStep.name()
                + "' must declare an input/inputTypeName contract.");
            return Optional.empty();
        }
        if (isBlank(templateStep.outputTypeName())) {
            error(ctx, "Branch-aware step '" + templateStep.name()
                + "' must declare an output/outputTypeName contract.");
            return Optional.empty();
        }

        Set<String> inputLeafTypes = expandLeafTypes(ctx, templateConfig, templateStep.inputTypeName(),
            templateStep.name(), "inputTypeName", false);
        if (inputLeafTypes == null) {
            return Optional.empty();
        }
        Set<String> outputLeafTypes = expandLeafTypes(ctx, templateConfig, templateStep.outputTypeName(),
            templateStep.name(), "outputTypeName", false);
        if (outputLeafTypes == null) {
            return Optional.empty();
        }

        boolean oneToOneCardinality = "ONE_TO_ONE".equalsIgnoreCase(templateStep.cardinality());
        if (!oneToOneCardinality
            && (templateStep.terminal()
                || !templateStep.accepts().isEmpty()
                || inputLeafTypes.size() != 1
                || outputLeafTypes.size() != 1)) {
            error(ctx, "Branch-aware routing currently supports ONE_TO_ONE steps once type-based routing is in play. Step '"
                + templateStep.name() + "' declares cardinality '" + templateStep.cardinality() + "'.");
            return Optional.empty();
        }

        Set<String> acceptedLeafTypes = new LinkedHashSet<>();
        if (!templateStep.accepts().isEmpty()) {
            for (String accepted : templateStep.accepts()) {
                if (isBlank(accepted)) {
                    error(ctx, "Step '" + templateStep.name() + "' declares a blank accepts entry.");
                    return Optional.empty();
                }
                if (templateConfig.unions().containsKey(accepted)) {
                    error(ctx, "Step '" + templateStep.name() + "' accepts '" + accepted
                        + "', but accepts may reference only concrete contract types, not unions.");
                    return Optional.empty();
                }
                Set<String> acceptedLeaf = expandLeafTypes(ctx, templateConfig, accepted, templateStep.name(), "accepts", true);
                if (acceptedLeaf == null) {
                    return Optional.empty();
                }
                acceptedLeafTypes.addAll(acceptedLeaf);
            }
            if (!inputLeafTypes.containsAll(acceptedLeafTypes)) {
                error(ctx, "Step '" + templateStep.name() + "' accepts " + acceptedLeafTypes
                    + " but inputTypeName '" + templateStep.inputTypeName() + "' resolves to " + inputLeafTypes + ".");
                return Optional.empty();
            }
        } else {
            acceptedLeafTypes.addAll(inputLeafTypes);
        }

        if (templateStep.terminal() && outputLeafTypes.size() != 1) {
            error(ctx, "Terminal step '" + templateStep.name()
                + "' must declare one concrete terminal output type. outputTypeName '"
                + templateStep.outputTypeName() + "' resolves to " + outputLeafTypes + ".");
            return Optional.empty();
        }

        List<ClassName> acceptedDomainTypes = new ArrayList<>(acceptedLeafTypes.size());
        for (String acceptedLeafType : acceptedLeafTypes) {
            ClassName acceptedDomainType = contractRuntimeTypes.get(acceptedLeafType);
            if (acceptedDomainType == null) {
                error(ctx, "Step '" + templateStep.name() + "' accepted contract type '" + acceptedLeafType
                    + "' could not be resolved to a Java runtime type during branch routing validation.");
                return Optional.empty();
            }
            acceptedDomainTypes.add(acceptedDomainType);
        }
        if (!templateStep.accepts().isEmpty()
            && !validateAssignableAcceptedTypes(ctx, templateStep, stepDefinition, acceptedDomainTypes)) {
            return Optional.empty();
        }

        return Optional.of(new ResolvedStep(
            index,
            templateStep,
            stepDefinition,
            List.copyOf(inputLeafTypes),
            List.copyOf(acceptedLeafTypes),
            List.copyOf(outputLeafTypes),
            acceptedDomainTypes));
    }

    private Map<String, ClassName> indexContractRuntimeTypes(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig templateConfig,
        List<PipelineTemplateStep> templateSteps,
        Map<String, StepDefinition> definitionsByName
    ) {
        Map<String, ClassName> contractRuntimeTypes = new LinkedHashMap<>();
        for (PipelineTemplateStep templateStep : templateSteps) {
            if (templateStep == null || isBlank(templateStep.name())) {
                continue;
            }
            StepDefinition stepDefinition = definitionsByName.get(normalizeStepName(templateStep.name()));
            if (stepDefinition == null) {
                continue;
            }
            registerContractRuntimeType(ctx, templateConfig, templateStep.inputTypeName(), stepDefinition.inputType(), contractRuntimeTypes);
            registerContractRuntimeType(ctx, templateConfig, templateStep.outputTypeName(), stepDefinition.outputType(), contractRuntimeTypes);
        }
        return contractRuntimeTypes;
    }

    private void registerContractRuntimeType(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig templateConfig,
        String contractTypeName,
        ClassName runtimeType,
        Map<String, ClassName> contractRuntimeTypes
    ) {
        if (isBlank(contractTypeName) || runtimeType == null || templateConfig == null) {
            return;
        }
        if (templateConfig.messages().containsKey(contractTypeName)) {
            contractRuntimeTypes.putIfAbsent(contractTypeName, runtimeType);
            return;
        }
        PipelineTemplateUnion union = templateConfig.unions().get(contractTypeName);
        if (union == null) {
            return;
        }
        contractRuntimeTypes.putIfAbsent(contractTypeName, runtimeType);
        for (Map.Entry<String, PipelineTemplateUnionVariant> entry : union.variants().entrySet()) {
            String variantName = entry.getKey();
            PipelineTemplateUnionVariant variant = entry.getValue();
            if (variant == null || isBlank(variant.type())) {
                continue;
            }
            ClassName variantRuntimeType = ClassName.get(runtimeType.packageName(), variant.type());
            if (isResolvable(ctx, variantRuntimeType)) {
                contractRuntimeTypes.putIfAbsent(variant.type(), variantRuntimeType);
            } else {
                report(ctx, Diagnostic.Kind.WARNING, "Union contract '" + contractTypeName
                    + "' variant '" + variantName + "' maps to unresolved runtime type '"
                    + variantRuntimeType.canonicalName() + "'; skipping runtime type indexing.");
            }
        }
    }

    private boolean validateReachability(PipelineCompilationContext ctx, List<ResolvedStep> resolvedSteps) {
        ResolvedStep firstStep = resolvedSteps.getFirst();
        Set<String> reachable = new LinkedHashSet<>(firstStep.inputLeafTypes());
        for (ResolvedStep step : resolvedSteps) {
            Set<String> unreachable = new LinkedHashSet<>(step.acceptedLeafTypes());
            unreachable.removeAll(reachable);
            if (!unreachable.isEmpty()) {
                error(ctx, "Step '" + step.templateStep().name()
                    + "' accepts types that are not currently reachable: " + unreachable
                    + ". Currently reachable: " + reachable);
                return false;
            }

            Set<String> skipped = new LinkedHashSet<>(reachable);
            skipped.removeAll(step.acceptedLeafTypes());

            if (step.terminal() && !skipped.isEmpty()) {
                error(ctx, "Terminal step '" + step.templateStep().name()
                    + "' does not cover all reachable branch-end alternatives. Missing: " + skipped);
                return false;
            }

            Set<String> nextReachable = new LinkedHashSet<>(skipped);
            nextReachable.addAll(step.producedLeafTypes());
            reachable = nextReachable;
        }
        return true;
    }

    private Set<String> expandLeafTypes(
        PipelineCompilationContext ctx,
        PipelineTemplateConfig templateConfig,
        String contractTypeName,
        String stepName,
        String fieldName,
        boolean concreteOnly
    ) {
        if (templateConfig == null || isBlank(contractTypeName)) {
            error(ctx, "Step '" + stepName + "' must declare " + fieldName + ".");
            return null;
        }
        if (templateConfig.messages().containsKey(contractTypeName)) {
            return Set.of(contractTypeName);
        }
        PipelineTemplateUnion union = templateConfig.unions().get(contractTypeName);
        if (union != null) {
            if (concreteOnly) {
                error(ctx, "Step '" + stepName + "' " + fieldName + " entry '" + contractTypeName
                    + "' must be a concrete contract type, not a union.");
                return null;
            }
            LinkedHashSet<String> leafTypes = new LinkedHashSet<>();
            for (PipelineTemplateUnionVariant variant : union.variants().values()) {
                leafTypes.add(variant.type());
            }
            return leafTypes;
        }
        error(ctx, "Step '" + stepName + "' references unknown contract type '" + contractTypeName
            + "' in " + fieldName + ".");
        return null;
    }

    private boolean validateAssignableAcceptedTypes(
        PipelineCompilationContext ctx,
        PipelineTemplateStep templateStep,
        StepDefinition stepDefinition,
        List<ClassName> acceptedDomainTypes
    ) {
        if (stepDefinition.inputType() == null || acceptedDomainTypes.isEmpty()) {
            return true;
        }
        ProcessingEnvironment processingEnv = ctx.getProcessingEnv();
        if (processingEnv == null || processingEnv.getElementUtils() == null || processingEnv.getTypeUtils() == null) {
            return true;
        }
        TypeElement stepInputElement = processingEnv.getElementUtils().getTypeElement(stepDefinition.inputType().canonicalName());
        if (stepInputElement == null) {
            error(ctx, "Step '" + templateStep.name() + "' input type '" + stepDefinition.inputType().canonicalName()
                + "' could not be resolved during branch routing validation.");
            return false;
        }
        Types types = processingEnv.getTypeUtils();
        TypeMirror stepInputType = stepInputElement.asType();
        for (ClassName acceptedType : acceptedDomainTypes) {
            TypeElement acceptedElement = processingEnv.getElementUtils().getTypeElement(acceptedType.canonicalName());
            if (acceptedElement == null) {
                error(ctx, "Step '" + templateStep.name() + "' accepted type '" + acceptedType.canonicalName()
                    + "' could not be resolved during branch routing validation.");
                return false;
            }
            if (!types.isAssignable(acceptedElement.asType(), stepInputType)) {
                error(ctx, "Step '" + templateStep.name() + "' accepts '" + acceptedType.simpleName()
                    + "', but that type is not assignable to Java input type '"
                    + stepDefinition.inputType().canonicalName() + "'.");
                return false;
            }
        }
        return true;
    }

    private boolean isResolvable(PipelineCompilationContext ctx, ClassName type) {
        if (ctx == null || ctx.getProcessingEnv() == null || ctx.getProcessingEnv().getElementUtils() == null) {
            return true;
        }
        return ctx.getProcessingEnv().getElementUtils().getTypeElement(type.canonicalName()) != null;
    }

    private PipelineTemplateConfig requireTemplateConfig(PipelineCompilationContext ctx) {
        return ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig templateConfig ? templateConfig : null;
    }

    private void error(PipelineCompilationContext ctx, String message) {
        report(ctx, Diagnostic.Kind.ERROR, message);
    }

    private void report(PipelineCompilationContext ctx, Diagnostic.Kind kind, String message) {
        if (ctx == null || ctx.getProcessingEnv() == null) {
            return;
        }
        Messager messager = ctx.getProcessingEnv().getMessager();
        if (messager != null) {
            messager.printMessage(kind, message);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String normalizeStepName(String name) {
        if (name == null) {
            return "";
        }
        return name.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
    }

    private record ResolvedStep(
        int index,
        PipelineTemplateStep templateStep,
        StepDefinition stepDefinition,
        List<String> inputLeafTypes,
        List<String> acceptedLeafTypes,
        List<String> producedLeafTypes,
        List<ClassName> acceptedDomainTypes
    ) {
        boolean terminal() {
            return templateStep.terminal();
        }
    }
}
