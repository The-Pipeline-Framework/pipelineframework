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

package org.pipelineframework.transport.function;

/**
 * Policy controlling how transport idempotency keys are derived at function boundaries.
 */
public enum IdempotencyPolicy {
    /**
     * Derive deterministic transport keys from stable context/envelope identifiers.
     */
    CONTEXT_STABLE,
    /**
     * Legacy alias kept for compatibility. Semantically maps to {@link #CONTEXT_STABLE}.
     */
    @Deprecated(forRemoval = false)
    RANDOM,
    /**
     * Use caller-supplied key from {@code tpf.idempotency.key} when available.
     */
    EXPLICIT
}
