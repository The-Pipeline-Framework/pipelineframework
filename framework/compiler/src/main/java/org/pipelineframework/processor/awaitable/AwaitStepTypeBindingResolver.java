/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.pipelineframework.processor.awaitable;

import java.util.List;
import java.util.Optional;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateDialect;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeferredCompletionDefinition;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.routing.V3JavaTypeResolver;
import org.pipelineframework.processor.util.ImplementedGenericInterfaceResolver;

/** Resolves and validates the Java contracts of deferred completion independently of operation kind. */
public final class AwaitStepTypeBindingResolver {
    private static final String PROJECTOR = "org.pipelineframework.awaitable.AwaitCompletionProjector";

    private final ImplementedGenericInterfaceResolver genericInterfaces =
        new ImplementedGenericInterfaceResolver();

    public Optional<AwaitStepTypeBinding> resolve(PipelineCompilationContext ctx, StepDefinition step) {
        return resolve(ctx, step, false);
    }

    /** Resolves the canonical operation boundary used by a module that does not own the service implementation. */
    public Optional<AwaitStepTypeBinding> resolveCanonicalBoundary(
        PipelineCompilationContext ctx,
        StepDefinition step
    ) {
        return resolve(ctx, step, true);
    }

    private Optional<AwaitStepTypeBinding> resolve(
        PipelineCompilationContext ctx,
        StepDefinition step,
        boolean canonicalBoundary
    ) {
        DeferredCompletionDefinition completion = step.deferredCompletion().orElseThrow(() ->
            new IllegalArgumentException("Step '" + step.name() + "' has no deferred completion"));
        if (!(ctx.getPipelineTemplateConfig() instanceof PipelineTemplateConfig config)
            || config.dialect() != PipelineTemplateDialect.V3) {
            return explicitBinding(step, completion);
        }

        V3JavaTypeResolver javaTypes = new V3JavaTypeResolver(config);
        if (completion.callback().isPresent()) {
            var callback = completion.callback().orElseThrow();
            if (!validateCallbackBean(ctx, step.name(), callback.endpointResolverClass(), "ProviderCallbackEndpointResolver")
                || !validateCallbackBean(ctx, step.name(), callback.authenticatorClass(), "ProviderCallbackAuthenticator")) {
                return Optional.empty();
            }
        }
        Optional<ClassName> inferredOperationOutput = javaTypes.resolve(completion.operationOutputType());
        Optional<ClassName> providerOperationOutput = ctx.getResolvedProviderBoundaries().stream()
            .filter(boundary -> step.name().equals(boundary.boundary().stepName()))
            .flatMap(boundary -> boundary.representations().stream())
            .filter(representation -> completion.operationOutputType().equals(representation.domainType().name()))
            .flatMap(representation -> representation.representationType().stream())
            .map(ClassName::bestGuess)
            .findFirst();
        Optional<ClassName> operationOutput = canonicalBoundary
            ? inferredOperationOutput
            : completion.operationOutputJavaType().isPresent()
                ? completion.operationOutputJavaType()
                : providerOperationOutput.isPresent() ? providerOperationOutput : inferredOperationOutput;
        Optional<ClassName> finalOutput = config.steps().stream()
            .filter(candidate -> candidate != null && step.name().equals(candidate.name()))
            .findFirst()
            .flatMap(candidate -> javaTypes.resolve(candidate.outputTypeName()));
        if (operationOutput.isEmpty() || finalOutput.isEmpty()) {
            error(ctx, "Step '" + step.name()
                + "' could not resolve await.operationOutput or the pipeline-visible output to canonical Java types.");
            return Optional.empty();
        }
        Optional<ClassName> compilerOwnedOperationOutput = providerOperationOutput.isPresent()
            ? providerOperationOutput : inferredOperationOutput;
        if (!canonicalBoundary
            && completion.operationOutputJavaType().isPresent() && compilerOwnedOperationOutput.isPresent()
            && !completion.operationOutputJavaType().orElseThrow().equals(compilerOwnedOperationOutput.orElseThrow())) {
            error(ctx, "Step '" + step.name() + "' await.operationOutput.java type '"
                + completion.operationOutputJavaType().orElseThrow().canonicalName()
                + "' does not match compiler-owned canonical type '"
                + compilerOwnedOperationOutput.orElseThrow().canonicalName() + "'.");
            return Optional.empty();
        }
        if (step.outputType() != null && !step.outputType().equals(finalOutput.orElseThrow())) {
            error(ctx, "Step '" + step.name() + "' explicit java.output type '"
                + step.outputType().canonicalName() + "' does not match compiler-owned final output type '"
                + finalOutput.orElseThrow().canonicalName() + "'.");
            return Optional.empty();
        }

        Optional<TypeName> payloadType = completion.completion()
            .flatMap(projected -> javaTypes.resolve(projected.type()))
            .map(TypeName.class::cast);
        if (completion.completion().isPresent() && payloadType.isEmpty()) {
            error(ctx, "Step '" + step.name() + "' could not resolve await.completion.type '"
                + completion.completion().orElseThrow().type() + "'.");
            return Optional.empty();
        }
        Optional<ClassName> projectorContext = completion.callback().isPresent()
            ? Optional.ofNullable(step.inputType()).or(() -> config.steps().stream()
                .filter(candidate -> candidate != null && step.name().equals(candidate.name()))
                .findFirst().flatMap(candidate -> javaTypes.resolve(candidate.inputTypeName())))
            : operationOutput;
        if (projectorContext.isEmpty()) {
            error(ctx, "Step '" + step.name() + "' could not resolve the canonical completion context type.");
            return Optional.empty();
        }
        if (completion.completion().isPresent()
            && !validateProjector(ctx, step.name(), completion.completion().orElseThrow(),
                projectorContext.orElseThrow(),
                payloadType.orElseThrow(), finalOutput.orElseThrow())) {
            return Optional.empty();
        }
        return Optional.of(new AwaitStepTypeBinding(
            operationOutput.orElseThrow(), finalOutput.orElseThrow(),
            config.steps().stream()
                .filter(candidate -> candidate != null && step.name().equals(candidate.name()))
                .findFirst().orElseThrow().outputTypeName(),
            completion.completion().map(DeferredCompletionDefinition.CompletionProjectionDefinition::type),
            payloadType));
    }

    private Optional<AwaitStepTypeBinding> explicitBinding(
        StepDefinition step,
        DeferredCompletionDefinition completion
    ) {
        if (completion.operationOutputJavaType().isEmpty() || step.outputType() == null) {
            return Optional.empty();
        }
        return Optional.of(new AwaitStepTypeBinding(
            completion.operationOutputJavaType().orElseThrow(), step.outputType(),
            step.outputType().simpleName(), Optional.empty(), Optional.empty()));
    }

    private boolean validateProjector(
        PipelineCompilationContext ctx,
        String stepName,
        DeferredCompletionDefinition.CompletionProjectionDefinition completion,
        ClassName expectedOperationOutput,
        TypeName expectedCompletion,
        ClassName expectedFinalOutput
    ) {
        ProcessingEnvironment processingEnv = ctx.getProcessingEnv();
        TypeElement projector = processingEnv.getElementUtils()
            .getTypeElement(completion.projector().canonicalName());
        if (projector == null) {
            error(ctx, "Step '" + stepName + "' completion projector '"
                + completion.projector().canonicalName() + "' was not found.");
            return false;
        }
        if (!validProjectorClass(ctx, stepName, projector)) {
            return false;
        }
        Optional<DeclaredType> projectorInterface = genericInterfaces.resolve(projector, PROJECTOR, processingEnv);
        if (projectorInterface.isEmpty()) {
            error(ctx, "Step '" + stepName + "' completion projector '"
                + completion.projector().canonicalName()
                + "' must implement AwaitCompletionProjector<I, C, O>.");
            return false;
        }
        List<? extends TypeMirror> arguments = projectorInterface.orElseThrow().getTypeArguments();
        if (arguments.size() != 3 || arguments.stream().anyMatch(argument -> !(argument instanceof DeclaredType declared)
            || !declared.getTypeArguments().isEmpty())) {
            error(ctx, "Step '" + stepName + "' completion projector must declare concrete I, C and O types.");
            return false;
        }
        return requireSame(ctx, stepName, "I", arguments.get(0), expectedOperationOutput)
            && requireSame(ctx, stepName, "C", arguments.get(1), expectedCompletion)
            && requireAssignable(ctx, stepName, "O", arguments.get(2), expectedFinalOutput);
    }

    private boolean validProjectorClass(PipelineCompilationContext ctx, String stepName, TypeElement projector) {
        boolean valid = projector.getKind() == ElementKind.CLASS
            && projector.getModifiers().contains(Modifier.PUBLIC)
            && !projector.getModifiers().contains(Modifier.ABSTRACT)
            && (projector.getNestingKind() != NestingKind.MEMBER
                || projector.getModifiers().contains(Modifier.STATIC));
        List<ExecutableElement> constructors = projector.getEnclosedElements().stream()
            .filter(element -> element.getKind() == ElementKind.CONSTRUCTOR)
            .map(ExecutableElement.class::cast)
            .toList();
        valid = valid && (constructors.isEmpty() || constructors.stream().anyMatch(constructor ->
            constructor.getParameters().isEmpty() && constructor.getModifiers().contains(Modifier.PUBLIC)));
        if (!valid) {
            error(ctx, "Step '" + stepName + "' completion projector '"
                + projector.getQualifiedName() + "' must be a public concrete class with a public no-arg constructor.");
        }
        return valid;
    }

    private boolean validateCallbackBean(PipelineCompilationContext ctx, String stepName, ClassName name, String contract) {
        var elements = ctx.getProcessingEnv().getElementUtils();
        TypeElement bean = elements.getTypeElement(name.canonicalName());
        TypeElement spi = elements.getTypeElement("org.pipelineframework.connector." + contract);
        boolean valid = bean != null && spi != null && bean.getKind() == ElementKind.CLASS
            && bean.getModifiers().contains(Modifier.PUBLIC) && !bean.getModifiers().contains(Modifier.ABSTRACT)
            && ctx.getProcessingEnv().getTypeUtils().isAssignable(bean.asType(), spi.asType());
        if (!valid) {
            error(ctx, "Step '" + stepName + "' callback bean '" + name + "' must be a public concrete " + contract);
        }
        return valid;
    }

    private boolean requireSame(
        PipelineCompilationContext ctx,
        String stepName,
        String slot,
        TypeMirror actual,
        TypeName expected
    ) {
        TypeElement expectedElement = ctx.getProcessingEnv().getElementUtils().getTypeElement(expected.toString());
        boolean matches = expectedElement == null
            ? actual.toString().equals(expected.toString())
            : ctx.getProcessingEnv().getTypeUtils().isSameType(actual, expectedElement.asType());
        if (!matches) {
            error(ctx, "Step '" + stepName + "' completion projector generic " + slot
                + " expected '" + expected + "' but was '" + actual + "'.");
        }
        return matches;
    }

    private boolean requireAssignable(
        PipelineCompilationContext ctx,
        String stepName,
        String slot,
        TypeMirror actual,
        TypeName expected
    ) {
        TypeElement expectedElement = ctx.getProcessingEnv().getElementUtils().getTypeElement(expected.toString());
        boolean matches = expectedElement == null
            ? actual.toString().equals(expected.toString())
            : ctx.getProcessingEnv().getTypeUtils().isAssignable(actual, expectedElement.asType());
        if (!matches) {
            error(ctx, "Step '" + stepName + "' completion projector generic " + slot
                + " expected assignability to '" + expected + "' but was '" + actual + "'.");
        }
        return matches;
    }

    private void error(PipelineCompilationContext ctx, String message) {
        ctx.getCompilerDiagnostics().error(message);
    }
}
