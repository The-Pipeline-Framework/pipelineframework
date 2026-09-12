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
 * Marker exception for failures that should not be retried by pipeline steps.
 */
public class NonRetryableException extends RuntimeException {

    /**
     * Creates a new NonRetryableException with the given message.
     *
     * @param message the error message
     */
    public NonRetryableException(String message) {
        super(message);
    }

    /**
     * Creates a new NonRetryableException with the given message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
