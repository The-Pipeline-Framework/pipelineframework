/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.invocation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepManyToMany;
import org.pipelineframework.step.StepOneToMany;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.step.functional.ManyToOne;
import org.pipelineframework.telemetry.PipelineRunContextHolder;

/**
 * Adapts a statically linked child definition to the existing TPF step interfaces.
 *
 * <p>The compiler selects one adapter from the child definition's resolved cardinality. This
 * class neither discovers definitions nor subscribes to their reactive output.
 */
public final class PipelineInvocationSteps {

    private PipelineInvocationSteps() {
    }

    /** Returns whether a generated step is a local pipeline-composition adapter. */
    public static boolean isInvocationStep(Object step) {
        return step instanceof InvocationStep;
    }

    private interface InvocationStep {
    }

    public static <I, O> StepOneToOne<I, O> oneToOne(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return oneToOne(runner, "$root", -1, linkedChildSteps);
    }

    public static <I, O> StepOneToOne<I, O> oneToOne(
        PipelineRunner runner,
        String definitionId,
        int definitionTerminalStepIndex,
        List<Object> linkedChildSteps
    ) {
        return new OneToOneInvocationStep<>(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps);
    }

    /**
     * Creates one recursive call adapter bound to the active parent invocation context.
     *
     * <p>Generated invocation beans call this factory from each reactive application; they must not
     * cache the returned adapter across parent invocations. The parent is captured from
     * {@link PipelineInvocationContextHolder}, and the child frame is entered when the returned
     * {@link Uni} is subscribed. Recursive descent and return signals use Mutiny's default executor
     * as a trampoline so the configured depth does not consume the JVM call stack.
     *
     * @throws IllegalStateException when no parent invocation context is active
     */
    public static <I, O> StepOneToOne<I, O> recursiveOneToOne(
        PipelineRunner runner,
        String definitionId,
        String callsiteId,
        int definitionTerminalStepIndex,
        List<Object> linkedChildSteps
    ) {
        PipelineInvocationContext parentContext = PipelineInvocationContextHolder.get()
            .orElseThrow(() -> new IllegalStateException(
                "Recursive pipeline invocation executed without an invocation context"));
        return new OneToOneInvocationStep<>(
            runner, definitionId, definitionTerminalStepIndex, linkedChildSteps,
            Optional.of(new RecursiveCall(required(callsiteId, "callsiteId"), parentContext)));
    }

    public static <I, O> StepOneToOne<I, O> recursiveOneToOne(
        PipelineRunner runner,
        String definitionId,
        String callsiteId,
        int definitionTerminalStepIndex,
        List<Object> linkedChildSteps,
        StepConfig stepConfig
    ) {
        StepOneToOne<I, O> step = recursiveOneToOne(
            runner, definitionId, callsiteId, definitionTerminalStepIndex, linkedChildSteps);
        ((ConfigurableStep) step).initialiseWithConfig(
            Objects.requireNonNull(stepConfig, "stepConfig must not be null"));
        return step;
    }

    public static <I, O> StepOneToMany<I, O> oneToMany(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return oneToMany(runner, "$root", -1, linkedChildSteps);
    }

    public static <I, O> StepOneToMany<I, O> oneToMany(
        PipelineRunner runner,
        String definitionId,
        int definitionTerminalStepIndex,
        List<Object> linkedChildSteps
    ) {
        return new OneToManyInvocationStep<>(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps);
    }

    public static <I, O> ManyToOne<I, O> manyToOne(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return manyToOne(runner, "$root", -1, linkedChildSteps);
    }

    public static <I, O> ManyToOne<I, O> manyToOne(
        PipelineRunner runner,
        String definitionId,
        int definitionTerminalStepIndex,
        List<Object> linkedChildSteps
    ) {
        return new ManyToOneInvocationStep<>(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps);
    }

    public static <I, O> StepManyToMany<I, O> manyToMany(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return manyToMany(runner, "$root", -1, linkedChildSteps);
    }

    public static <I, O> StepManyToMany<I, O> manyToMany(
        PipelineRunner runner,
        String definitionId,
        int definitionTerminalStepIndex,
        List<Object> linkedChildSteps
    ) {
        return new ManyToManyInvocationStep<>(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps);
    }

    private static final class OneToOneInvocationStep<I, O> extends ConfigurableStep
        implements StepOneToOne<I, O>, InvocationStep {

        private final PipelineRunner runner;
        private final String definitionId;
        private final int definitionTerminalStepIndex;
        private final List<Object> linkedChildSteps;
        private final Optional<RecursiveCall> recursiveCall;

        private OneToOneInvocationStep(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
                List<Object> linkedChildSteps) {
            this(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps, Optional.empty());
        }

        private OneToOneInvocationStep(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
                List<Object> linkedChildSteps, Optional<RecursiveCall> recursiveCall) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.definitionId = definitionId(definitionId);
            this.definitionTerminalStepIndex = definitionTerminalStepIndex;
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
            this.recursiveCall = Objects.requireNonNull(recursiveCall, "recursiveCall must not be null");
        }

        @Override
        @SuppressWarnings("unchecked")
        public Uni<O> applyOneToOne(I input) {
            if (recursiveCall.isPresent()) {
                return Uni.createFrom().deferred(() -> {
                    PipelineInvocationContext active = activeParentContext(recursiveCall.orElseThrow());
                    return Uni.createFrom().deferred(() -> invokeOneToOne(input, Optional.of(active)))
                        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
                        .emitOn(Infrastructure.getDefaultExecutor());
                });
            }
            return invokeOneToOne(input, Optional.empty());
        }

        @SuppressWarnings("unchecked")
        private Uni<O> invokeOneToOne(I input, Optional<PipelineInvocationContext> parentContext) {
            Optional<PipelineInvocationContext> childContext = recursiveCall.map(call ->
                parentContext.orElseThrow().enterRecursive(definitionId, call.callsiteId()));
            Object result = nestedResult(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps,
                Uni.createFrom().item(input), childContext);
            if (result instanceof Uni<?> uni) {
                return (Uni<O>) uni;
            }
            throw new IllegalStateException("Linked ONE_TO_ONE pipeline returned a streaming result");
        }

        private PipelineInvocationContext activeParentContext(RecursiveCall call) {
            PipelineInvocationContext active = PipelineInvocationContextHolder.get()
                .orElseThrow(() -> new IllegalStateException(
                    "Recursive pipeline invocation subscribed without an invocation context"));
            if (!active.equals(call.parentContext())) {
                throw new IllegalStateException(
                    "Recursive pipeline invocation adapter does not belong to the active parent invocation");
            }
            return active;
        }
    }

    private record RecursiveCall(String callsiteId, PipelineInvocationContext parentContext) {
        private RecursiveCall {
            callsiteId = required(callsiteId, "callsiteId");
            parentContext = Objects.requireNonNull(parentContext, "parentContext must not be null");
        }
    }

    private static final class OneToManyInvocationStep<I, O> extends ConfigurableStep
        implements StepOneToMany<I, O>, InvocationStep {

        private final PipelineRunner runner;
        private final String definitionId;
        private final int definitionTerminalStepIndex;
        private final List<Object> linkedChildSteps;

        private OneToManyInvocationStep(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
                List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.definitionId = definitionId(definitionId);
            this.definitionTerminalStepIndex = definitionTerminalStepIndex;
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Multi<O> applyOneToMany(I input) {
            Object result = nestedResult(
                runner, definitionId, definitionTerminalStepIndex, linkedChildSteps, Uni.createFrom().item(input));
            if (result instanceof Multi<?> multi) {
                return (Multi<O>) multi;
            }
            throw new IllegalStateException("Linked ONE_TO_MANY pipeline returned a unary result");
        }
    }

    private static final class ManyToOneInvocationStep<I, O> extends ConfigurableStep
        implements ManyToOne<I, O>, InvocationStep {

        private final PipelineRunner runner;
        private final String definitionId;
        private final int definitionTerminalStepIndex;
        private final List<Object> linkedChildSteps;

        private ManyToOneInvocationStep(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
                List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.definitionId = definitionId(definitionId);
            this.definitionTerminalStepIndex = definitionTerminalStepIndex;
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Uni<O> apply(Multi<I> input) {
            Object result = nestedResult(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps, input);
            if (result instanceof Uni<?> uni) {
                return (Uni<O>) uni;
            }
            throw new IllegalStateException("Linked MANY_TO_ONE pipeline returned a streaming result");
        }
    }

    private static final class ManyToManyInvocationStep<I, O> extends ConfigurableStep
        implements StepManyToMany<I, O>, InvocationStep {

        private final PipelineRunner runner;
        private final String definitionId;
        private final int definitionTerminalStepIndex;
        private final List<Object> linkedChildSteps;

        private ManyToManyInvocationStep(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
                List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.definitionId = definitionId(definitionId);
            this.definitionTerminalStepIndex = definitionTerminalStepIndex;
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Multi<O> applyTransform(Multi<I> input) {
            Object result = nestedResult(runner, definitionId, definitionTerminalStepIndex, linkedChildSteps, input);
            if (result instanceof Multi<?> multi) {
                return (Multi<O>) multi;
            }
            throw new IllegalStateException("Linked MANY_TO_MANY pipeline returned a unary result");
        }
    }

    private static Object nestedResult(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
            List<Object> linkedChildSteps, Object input) {
        return nestedResult(
            runner, definitionId, definitionTerminalStepIndex, linkedChildSteps, input, Optional.empty());
    }

    private static Object nestedResult(PipelineRunner runner, String definitionId, int definitionTerminalStepIndex,
            List<Object> linkedChildSteps, Object input, Optional<PipelineInvocationContext> invocationContext) {
        Optional<org.pipelineframework.telemetry.PipelineRunContext> runContext = PipelineRunContextHolder.get();
        PipelineRunner.ExecutionResult execution;
        if (invocationContext.isPresent() && runContext.isPresent()) {
            execution = runner.runNestedWithContext(
                input,
                linkedChildSteps,
                definitionId,
                definitionTerminalStepIndex,
                invocationContext.orElseThrow(),
                runContext.orElseThrow());
        } else if (invocationContext.isPresent()) {
            execution = runner.runNestedWithContext(
                input, linkedChildSteps, definitionId, definitionTerminalStepIndex, invocationContext.orElseThrow());
        } else if (runContext.isPresent()) {
            execution = runner.runNestedWithContext(
                input, linkedChildSteps, definitionId, definitionTerminalStepIndex, runContext.orElseThrow());
        } else {
            execution = runner.runNestedWithContext(
                input, linkedChildSteps, definitionId, definitionTerminalStepIndex);
        }
        if (execution.terminalOutputPublished()) {
            throw new IllegalStateException("Nested pipeline invocation must not own terminal publication");
        }
        return execution.result();
    }

    private static String definitionId(String value) {
        return required(value, "definitionId");
    }

    private static String required(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
