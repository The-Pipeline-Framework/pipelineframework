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

package org.pipelineframework.step;


/**
 * Interface for side effect pipeline steps that perform operations with side effects
 * but pass the original input item downstream unchanged.
 * 
 * <p>This interface represents a 1 -> side-effect (async) transformation where an input item
 * triggers an asynchronous side effect operation, but the original item continues down
 * the pipeline unchanged.</p>
 * 
 * @param <I> the type of input item
 */
public interface StepSideEffect<I> extends Configurable, StepOneToOne<I, I>, DeadLetterQueue<I, I> {

    /**
     * Indicates whether this step should run with virtual threads.
     * @return true if virtual threads should be used, false otherwise (defaults to false)
     */
}
