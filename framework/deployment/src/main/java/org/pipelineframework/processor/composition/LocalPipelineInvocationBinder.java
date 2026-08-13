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

package org.pipelineframework.processor.composition;

import java.util.List;
import java.util.Objects;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.invocation.PipelineInvocationDescriptor;
import org.pipelineframework.invocation.PipelineInvocationSteps;

/**
 * Builds a local runtime step from an already linked static pipeline invocation.
 *
 * <p>This binder consumes {@link PipelineInvocationBinding}; it does not inspect YAML, classpath
 * resources, or a runtime definition registry. A generated local binding can supply the child
 * step instances directly.
 */
public final class LocalPipelineInvocationBinder implements PipelineInvocationRealization<Object> {

    private final PipelineRunner runner;
    private final List<Object> linkedChildSteps;

    public LocalPipelineInvocationBinder(PipelineRunner runner, List<Object> linkedChildSteps) {
        this.runner = Objects.requireNonNull(runner, "runner must not be null");
        this.linkedChildSteps = List.copyOf(Objects.requireNonNull(
            linkedChildSteps,
            "linkedChildSteps must not be null"));
    }

    @Override
    public Object realize(PipelineInvocationBinding binding) {
        Objects.requireNonNull(binding, "binding must not be null");
        PipelineInvocationDescriptor descriptor = descriptorFor(binding);
        return switch (binding.cardinality()) {
            case ONE_TO_ONE -> PipelineInvocationSteps.oneToOne(runner, linkedChildSteps, descriptor);
            case ONE_TO_MANY -> PipelineInvocationSteps.oneToMany(runner, linkedChildSteps, descriptor);
            case MANY_TO_ONE -> PipelineInvocationSteps.manyToOne(runner, linkedChildSteps, descriptor);
            case MANY_TO_MANY -> PipelineInvocationSteps.manyToMany(runner, linkedChildSteps, descriptor);
        };
    }

    private PipelineInvocationDescriptor descriptorFor(PipelineInvocationBinding binding) {
        String invocation = binding.invocationLocation().display();
        if (binding.childStepLocations().size() != linkedChildSteps.size()) {
            throw new IllegalArgumentException("Linked child runtime steps do not match the compiled child locations");
        }
        List<String> childLocations = binding.childStepLocations().stream()
            .map(location -> location.display())
            .toList();
        return new PipelineInvocationDescriptor(invocation, childLocations);
    }
}
