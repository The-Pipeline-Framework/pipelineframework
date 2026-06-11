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

package org.pipelineframework.runtime.core;

/**
 * Neutral contract for transaction boundary handling in a runtime that supports one.
 */
public interface TransactionBoundary {

    /**
     * Wraps callback execution in a transaction boundary if available.
     *
     * @param callback callback to execute
     * @param <T> callback return type
     * @return callback result
     */
    <T> T executeInTransaction(java.util.concurrent.Callable<T> callback);
}
