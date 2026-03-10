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

package org.pipelineframework.reject;

/**
 * Policy used when publishing to the item reject sink fails.
 */
public enum ItemRejectFailurePolicy {
    /**
     * Continue pipeline execution even when reject publication fails.
     */
    CONTINUE,
    /**
     * Propagate publication failure and fail pipeline execution.
     */
    FAIL_PIPELINE
}
