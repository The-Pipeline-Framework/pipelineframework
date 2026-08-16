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
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.PipelineRunner;
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

    public static <I, O> StepOneToOne<I, O> oneToOne(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return new OneToOneInvocationStep<>(runner, linkedChildSteps);
    }

    public static <I, O> StepOneToMany<I, O> oneToMany(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return new OneToManyInvocationStep<>(runner, linkedChildSteps);
    }

    public static <I, O> ManyToOne<I, O> manyToOne(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return new ManyToOneInvocationStep<>(runner, linkedChildSteps);
    }

    public static <I, O> StepManyToMany<I, O> manyToMany(
        PipelineRunner runner,
        List<Object> linkedChildSteps
    ) {
        return new ManyToManyInvocationStep<>(runner, linkedChildSteps);
    }

    private static final class OneToOneInvocationStep<I, O> extends ConfigurableStep
        implements StepOneToOne<I, O> {

        private final PipelineRunner runner;
        private final List<Object> linkedChildSteps;

        private OneToOneInvocationStep(PipelineRunner runner, List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Uni<O> applyOneToOne(I input) {
            Object result = nestedResult(runner, linkedChildSteps, Uni.createFrom().item(input));
            if (result instanceof Uni<?> uni) {
                return (Uni<O>) uni;
            }
            throw new IllegalStateException("Linked ONE_TO_ONE pipeline returned a streaming result");
        }
    }

    private static final class OneToManyInvocationStep<I, O> extends ConfigurableStep
        implements StepOneToMany<I, O> {

        private final PipelineRunner runner;
        private final List<Object> linkedChildSteps;

        private OneToManyInvocationStep(PipelineRunner runner, List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Multi<O> applyOneToMany(I input) {
            Object result = nestedResult(runner, linkedChildSteps, Uni.createFrom().item(input));
            if (result instanceof Multi<?> multi) {
                return (Multi<O>) multi;
            }
            throw new IllegalStateException("Linked ONE_TO_MANY pipeline returned a unary result");
        }
    }

    private static final class ManyToOneInvocationStep<I, O> extends ConfigurableStep
        implements ManyToOne<I, O> {

        private final PipelineRunner runner;
        private final List<Object> linkedChildSteps;

        private ManyToOneInvocationStep(PipelineRunner runner, List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Uni<O> apply(Multi<I> input) {
            Object result = nestedResult(runner, linkedChildSteps, input);
            if (result instanceof Uni<?> uni) {
                return (Uni<O>) uni;
            }
            throw new IllegalStateException("Linked MANY_TO_ONE pipeline returned a streaming result");
        }
    }

    private static final class ManyToManyInvocationStep<I, O> extends ConfigurableStep
        implements StepManyToMany<I, O> {

        private final PipelineRunner runner;
        private final List<Object> linkedChildSteps;

        private ManyToManyInvocationStep(PipelineRunner runner, List<Object> linkedChildSteps) {
            this.runner = Objects.requireNonNull(runner, "runner must not be null");
            this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
                linkedChildSteps,
                "linkedChildSteps must not be null"));
        }

        @Override
        @SuppressWarnings("unchecked")
        public Multi<O> applyTransform(Multi<I> input) {
            Object result = nestedResult(runner, linkedChildSteps, input);
            if (result instanceof Multi<?> multi) {
                return (Multi<O>) multi;
            }
            throw new IllegalStateException("Linked MANY_TO_MANY pipeline returned a unary result");
        }
    }

    private static Object nestedResult(PipelineRunner runner, List<Object> linkedChildSteps, Object input) {
        PipelineRunner.ExecutionResult execution = PipelineRunContextHolder.get()
            .map(context -> runner.runNestedWithContext(input, linkedChildSteps, context))
            .orElseGet(() -> runner.runNestedWithContext(input, linkedChildSteps));
        if (execution.terminalOutputPublished()) {
            throw new IllegalStateException("Nested pipeline invocation must not own terminal publication");
        }
        return execution.result();
    }
}
