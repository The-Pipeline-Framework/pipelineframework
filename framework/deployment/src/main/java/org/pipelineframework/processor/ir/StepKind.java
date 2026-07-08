/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.processor.ir;

/**
 * Enum representing the kind of step: internal, delegated, or remote.
 */
public enum StepKind {
    /**
     * An internal step where the execution service is implemented within the application
     * and bound from YAML or legacy @PipelineStep metadata.
     */
    INTERNAL,
    
    /**
     * A delegated step where the execution is provided by an operator service
     * that is not annotated with @PipelineStep.
     */
    DELEGATED,

    /**
     * A remote step whose execution is delegated to an external operator endpoint
     * resolved from v2 template execution metadata.
     */
    REMOTE,

    /**
     * An await step that dispatches external work, stores durable interaction state,
     * and resumes from a correlated completion.
     */
    AWAIT,

    /**
     * A command step that executes an idempotent external effect through a managed connector.
     */
    COMMAND,

    /**
     * A captured query step that reads through an application connector and makes
     * decision-affecting external state an explicit pipeline input.
     */
    QUERY
}
