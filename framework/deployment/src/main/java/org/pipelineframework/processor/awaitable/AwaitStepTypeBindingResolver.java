/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor.awaitable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateDialect;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.routing.PipelineBranchingPlan;
import org.pipelineframework.processor.routing.V3JavaTypeResolver;
import org.pipelineframework.processor.util.ImplementedGenericInterfaceResolver;

/** Resolves and validates the Java boundary of a v3 Await step from compiler-owned semantics. */
public final class AwaitStepTypeBindingResolver {
    private static final String PROJECTOR = "org.pipelineframework.awaitable.AwaitCompletionProjector";

    private final ImplementedGenericInterfaceResolver genericInterfaces =
        new ImplementedGenericInterfaceResolver();

    public Optional<AwaitStepTypeBinding> resolve(PipelineCompilationContext ctx, StepDefinition step) {
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config)
            || config.dialect() != PipelineTemplateDialect.V3) {
            return explicitBinding(step);
        }
        Optional<PipelineTemplateStep> authoredStep = config.steps().stream()
            .filter(candidate -> candidate != null && step.name().equals(candidate.name()))
            .findFirst();
        if (authoredStep.isEmpty()) {
            return explicitBinding(step);
        }

        V3JavaTypeResolver javaTypes = new V3JavaTypeResolver(config);
        Optional<ClassName> inferredInput = effectiveInputType(
            config, authoredStep.orElseThrow(), ctx.getBranchingPlan(), javaTypes);
        Optional<ClassName> inferredOutput = javaTypes.resolve(authoredStep.orElseThrow().outputTypeName());
        if (inferredInput.isEmpty() || inferredOutput.isEmpty()) {
            error(ctx, "Await step '" + step.name()
                + "' could not resolve its v3 input/output contracts to canonical Java types.");
            return Optional.empty();
        }
        if (!compatibleExplicitType(ctx, step.name(), "input", step.inputType(), inferredInput.orElseThrow())) {
            return Optional.empty();
        }
        if (!compatibleExplicitType(ctx, step.name(), "output", step.outputType(), inferredOutput.orElseThrow())) {
            return Optional.empty();
        }

        CompletionConfigResolution completion = completionConfig(ctx, step.name(), step.awaitConfig());
        if (completion == CompletionConfigState.INVALID) {
            return Optional.empty();
        }
        if (completion == CompletionConfigState.ABSENT) {
            return Optional.of(new AwaitStepTypeBinding(inferredInput.orElseThrow(), inferredOutput.orElseThrow()));
        }
        return validateProjector(ctx, step, authoredStep.orElseThrow(), javaTypes,
            inferredInput.orElseThrow(), inferredOutput.orElseThrow(),
            ((ResolvedCompletionConfig) completion).config());
    }

    private Optional<AwaitStepTypeBinding> validateProjector(
        PipelineCompilationContext ctx,
        StepDefinition step,
        PipelineTemplateStep authoredStep,
        V3JavaTypeResolver javaTypes,
        ClassName expectedInput,
        ClassName inferredOutput,
        CompletionConfig completion
    ) {
        ProcessingEnvironment processingEnv = ctx.getProcessingEnv();
        TypeElement projector = processingEnv.getElementUtils().getTypeElement(completion.projectorType());
        if (projector == null) {
            error(ctx, "Await step '" + step.name() + "' completion projector '"
                + completion.projectorType() + "' was not found.");
            return Optional.empty();
        }
        if (!validProjectorClass(ctx, step.name(), projector)) {
            return Optional.empty();
        }
        Optional<DeclaredType> projectorInterface = genericInterfaces.resolve(projector, PROJECTOR, processingEnv);
        if (projectorInterface.isEmpty()) {
            error(ctx, "Await step '" + step.name() + "' completion projector '"
                + completion.projectorType() + "' must implement AwaitCompletionProjector<I, C, O>.");
            return Optional.empty();
        }
        List<? extends TypeMirror> arguments = projectorInterface.orElseThrow().getTypeArguments();
        if (arguments.size() != 3 || arguments.stream().anyMatch(argument -> !isConcreteBoundaryType(argument))) {
            error(ctx, "Await step '" + step.name() + "' completion projector '"
                + completion.projectorType()
                + "' must declare concrete, non-parameterized AwaitCompletionProjector<I, C, O> arguments.");
            return Optional.empty();
        }

        TypeMirror inputArgument = arguments.get(0);
        TypeMirror completionArgument = arguments.get(1);
        TypeMirror outputArgument = arguments.get(2);
        if (!sameType(processingEnv, inputArgument, expectedInput)) {
            return genericMismatch(ctx, step.name(), completion.projectorType(), "I", expectedInput,
                TypeName.get(inputArgument));
        }
        TypeElement completionElement = processingEnv.getElementUtils().getTypeElement(completion.completionType());
        if (completionElement == null) {
            error(ctx, "Await step '" + step.name() + "' completion type '"
                + completion.completionType() + "' was not found.");
            return Optional.empty();
        }
        if (!processingEnv.getTypeUtils().isSameType(completionArgument, completionElement.asType())) {
            return genericMismatch(ctx, step.name(), completion.projectorType(), "C",
                ClassName.get(completionElement), TypeName.get(completionArgument));
        }

        ClassName outputClass = className(outputArgument).orElseThrow();
        ClassName expectedJavaOutput = step.outputType() == null ? inferredOutput : step.outputType();
        if (!assignableTo(processingEnv, outputArgument, expectedJavaOutput)) {
            return genericMismatch(ctx, step.name(), completion.projectorType(), "O",
                expectedJavaOutput, outputClass);
        }
        Optional<String> outputSemanticType = javaTypes.semanticType(outputClass);
        if (step.outputType() == null
            && !outputSemanticType.map(semanticType ->
                    ((PipelineTemplateConfig) ctx.getPipelineTemplateConfig()).typeModel()
                        .isAssignable(semanticType, authoredStep.outputTypeName()))
                .orElse(outputClass.equals(inferredOutput))) {
            error(ctx, "Await step '" + step.name() + "' completion projector '"
                + completion.projectorType() + "' generic O type '" + outputClass.canonicalName()
                + "' is not semantically assignable to declared v3 output '"
                + authoredStep.outputTypeName() + "'.");
            return Optional.empty();
        }
        return Optional.of(new AwaitStepTypeBinding(expectedInput, outputClass));
    }

    private Optional<ClassName> effectiveInputType(
        PipelineTemplateConfig config,
        PipelineTemplateStep step,
        PipelineBranchingPlan plan,
        V3JavaTypeResolver javaTypes
    ) {
        if (isUnion(config, step.inputTypeName()) && !step.accepts().isEmpty() && plan != null && plan.branchAware()) {
            Optional<PipelineBranchingPlan.BranchStep> planned = plan.steps().stream()
                .filter(candidate -> step.name().equals(candidate.stepName()))
                .findFirst();
            if (planned.isPresent() && planned.orElseThrow().acceptedDomainTypes().size() == 1) {
                return Optional.of(planned.orElseThrow().acceptedDomainTypes().getFirst());
            }
        }
        return javaTypes.resolve(step.inputTypeName());
    }

    private boolean isUnion(PipelineTemplateConfig config, String semanticType) {
        PipelineTemplateTypeReference resolved = config.typeModel().resolveAliases(
            new PipelineTemplateTypeReference.Named(semanticType));
        return resolved instanceof PipelineTemplateTypeReference.Named named
            && config.typeModel().definition(named.name())
                .filter(PipelineTemplateTypeDefinition.UnionType.class::isInstance)
                .isPresent();
    }

    private boolean validProjectorClass(PipelineCompilationContext ctx, String stepName, TypeElement projector) {
        boolean publicConcreteClass = projector.getModifiers().contains(Modifier.PUBLIC)
            && projector.getKind() == ElementKind.CLASS
            && !projector.getModifiers().contains(Modifier.ABSTRACT)
            && (projector.getNestingKind() != NestingKind.MEMBER
                || projector.getModifiers().contains(Modifier.STATIC));
        if (!publicConcreteClass) {
            error(ctx, "Await step '" + stepName + "' completion projector '"
                + projector.getQualifiedName() + "' must be a public concrete class.");
            return false;
        }
        List<ExecutableElement> constructors = projector.getEnclosedElements().stream()
            .filter(element -> element.getKind() == ElementKind.CONSTRUCTOR)
            .map(ExecutableElement.class::cast)
            .toList();
        boolean hasPublicNoArg = constructors.isEmpty() || constructors.stream()
            .anyMatch(constructor -> constructor.getParameters().isEmpty()
                && constructor.getModifiers().contains(Modifier.PUBLIC));
        if (!hasPublicNoArg) {
            error(ctx, "Await step '" + stepName + "' completion projector '"
                + projector.getQualifiedName() + "' must declare a public no-argument constructor.");
        }
        return hasPublicNoArg;
    }

    private boolean isConcreteBoundaryType(TypeMirror type) {
        if (!(type instanceof DeclaredType declared) || !declared.getTypeArguments().isEmpty()) {
            return false;
        }
        return declared.asElement() instanceof TypeElement element && element.getTypeParameters().isEmpty();
    }

    private boolean compatibleExplicitType(
        PipelineCompilationContext ctx,
        String stepName,
        String slot,
        ClassName explicit,
        ClassName inferred
    ) {
        if (explicit == null || explicit.equals(inferred)) {
            return true;
        }
        error(ctx, "Await step '" + stepName + "' explicit java." + slot + " type '"
            + explicit.canonicalName() + "' does not match compiler-inferred v3 " + slot
            + " type '" + inferred.canonicalName() + "'.");
        return false;
    }

    private boolean sameType(ProcessingEnvironment processingEnv, TypeMirror actual, ClassName expected) {
        TypeElement expectedElement = processingEnv.getElementUtils().getTypeElement(expected.canonicalName());
        return expectedElement == null
            ? actual.toString().equals(expected.canonicalName())
            : processingEnv.getTypeUtils().isSameType(actual, expectedElement.asType());
    }

    private boolean assignableTo(ProcessingEnvironment processingEnv, TypeMirror actual, ClassName expected) {
        TypeElement expectedElement = processingEnv.getElementUtils().getTypeElement(expected.canonicalName());
        return expectedElement == null
            ? actual.toString().equals(expected.canonicalName())
            : processingEnv.getTypeUtils().isAssignable(actual, expectedElement.asType());
    }

    private Optional<AwaitStepTypeBinding> genericMismatch(
        PipelineCompilationContext ctx,
        String stepName,
        String projector,
        String slot,
        TypeName expected,
        TypeName actual
    ) {
        error(ctx, "Await step '" + stepName + "' completion projector '" + projector
            + "' generic " + slot + " expected '" + expected + "' but was '" + actual + "'.");
        return Optional.empty();
    }

    private Optional<ClassName> className(TypeMirror type) {
        TypeName typeName = TypeName.get(type);
        return typeName instanceof ClassName className ? Optional.of(className) : Optional.empty();
    }

    private CompletionConfigResolution completionConfig(
        PipelineCompilationContext ctx,
        String stepName,
        Map<String, Object> awaitConfig
    ) {
        if (!awaitConfig.containsKey("completion")) {
            return CompletionConfigState.ABSENT;
        }
        Object value = awaitConfig.get("completion");
        if (!(value instanceof Map<?, ?> completion)) {
            error(ctx, "Await step '" + stepName + "' completion must be a map.");
            return CompletionConfigState.INVALID;
        }
        for (String requiredKey : List.of("type", "projector")) {
            if (!completion.containsKey(requiredKey)) {
                error(ctx, "Await step '" + stepName
                    + "' completion is missing required key '" + requiredKey + "'.");
                return CompletionConfigState.INVALID;
            }
            Object configured = completion.get(requiredKey);
            if (!(configured instanceof String text) || text.isBlank()) {
                error(ctx, "Await step '" + stepName + "' completion key '"
                    + requiredKey + "' must be a non-blank string.");
                return CompletionConfigState.INVALID;
            }
        }
        return new ResolvedCompletionConfig(new CompletionConfig(
            (String) completion.get("type"),
            (String) completion.get("projector")));
    }

    private Optional<AwaitStepTypeBinding> explicitBinding(StepDefinition step) {
        return step.inputType() == null || step.outputType() == null
            ? Optional.empty()
            : Optional.of(new AwaitStepTypeBinding(step.inputType(), step.outputType()));
    }

    private void error(PipelineCompilationContext ctx, String message) {
        ctx.getProcessingEnv().getMessager().printMessage(Diagnostic.Kind.ERROR, message);
    }

    private record CompletionConfig(String completionType, String projectorType) {
    }

    private sealed interface CompletionConfigResolution
        permits CompletionConfigState, ResolvedCompletionConfig {
    }

    private enum CompletionConfigState implements CompletionConfigResolution {
        ABSENT,
        INVALID
    }

    private record ResolvedCompletionConfig(CompletionConfig config)
        implements CompletionConfigResolution {
    }
}
