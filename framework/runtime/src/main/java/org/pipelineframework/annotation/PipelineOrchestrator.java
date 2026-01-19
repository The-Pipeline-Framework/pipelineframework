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

package org.pipelineframework.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation to trigger orchestrator endpoint generation.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface PipelineOrchestrator {
    /**
     * Enables CLI generation for the orchestrator.
     *
     * @return {@code true} to generate a CLI entrypoint, {@code false} to skip it
     */
    boolean generateCli() default true;

    /**
     * CLI command name.
     *
     * @return CLI command name, or empty to use the default
     */
    String name() default "";

    /**
     * CLI command description.
     *
     * @return CLI command description, or empty to use the default
     */
    String description() default "";

    /**
     * CLI command version.
     *
     * @return CLI command version, or empty to use the default
     */
    String version() default "";
}
