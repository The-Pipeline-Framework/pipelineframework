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
import java.util.OptionalInt;
import org.pipelineframework.orchestrator.PipelineExecutionPosition;

/**
 * Runtime projection of one compiler-linked static invocation.
 *
 * <p>The descriptor contains no placement policy or runner. A local realization uses it to map a
 * persisted static continuation location back to a child-step cursor; a future remote realization
 * can consume the same immutable semantic descriptor.
 *
 * @param invocationLocation compiler-derived callsite location
 * @param childStepLocations compiler-derived child locations in ordered execution order
 */
public record PipelineInvocationDescriptor(
    String invocationLocation,
    List<String> childStepLocations
) {

    public PipelineInvocationDescriptor {
        invocationLocation = requireNonBlank(invocationLocation, "invocationLocation");
        childStepLocations = List.copyOf(Objects.requireNonNull(childStepLocations, "childStepLocations must not be null"));
        for (String location : childStepLocations) {
            if (!requireNonBlank(location, "childStepLocations entry").startsWith(invocationLocation + "/")) {
                throw new IllegalArgumentException("Child step location must be rooted at invocationLocation");
            }
        }
    }

    public String terminalLocation() {
        return invocationLocation + "/$end";
    }

    public OptionalInt resumeStartIndex(PipelineExecutionPosition position) {
        Objects.requireNonNull(position, "position must not be null");
        if (!position.nested()) {
            return OptionalInt.empty();
        }
        int index = childStepLocations.indexOf(position.staticLocation());
        if (index >= 0) {
            return OptionalInt.of(index);
        }
        return terminalLocation().equals(position.staticLocation())
            ? OptionalInt.of(childStepLocations.size())
            : OptionalInt.empty();
    }

    public PipelineExecutionPosition positionAt(int rootStepIndex, int childStepIndex) {
        if (childStepIndex < 0 || childStepIndex >= childStepLocations.size()) {
            throw new IllegalArgumentException("childStepIndex is out of range: " + childStepIndex);
        }
        String next = childStepIndex + 1 < childStepLocations.size()
            ? childStepLocations.get(childStepIndex + 1)
            : terminalLocation();
        return PipelineExecutionPosition.nested(rootStepIndex, childStepLocations.get(childStepIndex), next);
    }

    private static String requireNonBlank(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
