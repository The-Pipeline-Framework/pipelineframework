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

import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Declares ordering and thread-safety hints for pipeline steps and plugins.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ParallelismHint {
    /**
     * Declares the ordering requirement for this plugin or step.
     *
     * @return ordering requirement
     */
    OrderingRequirement ordering() default OrderingRequirement.RELAXED;

    /**
     * Declares whether this plugin or step is safe to invoke concurrently.
     *
     * @return thread safety declaration
     */
    ThreadSafety threadSafety() default ThreadSafety.SAFE;
}
