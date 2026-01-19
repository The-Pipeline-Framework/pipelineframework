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

package org.pipelineframework.cache;

import org.pipelineframework.step.NonRetryableException;

/**
 * Raised when cache policy enforcement fails.
 */
public class CachePolicyViolation extends NonRetryableException {

    /**
     * Creates a new CachePolicyViolation with the given message.
     *
     * @param message the error message
     */
    public CachePolicyViolation(String message) {
        super(message);
    }
}
