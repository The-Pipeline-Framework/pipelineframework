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

package org.pipelineframework.context;

/**
 * Standard headers used for pipeline context propagation.
 */
public final class PipelineContextHeaders {

    /** Header name for pipeline version tags. */
    public static final String VERSION = "x-pipeline-version";
    /** Header name for pipeline replay mode. */
    public static final String REPLAY = "x-pipeline-replay";
    /** Header name for cache policy overrides. */
    public static final String CACHE_POLICY = "x-pipeline-cache-policy";
    /** Header name for cache status propagation. */
    public static final String CACHE_STATUS = "x-pipeline-cache-status";
    /** Canonical Protobuf-over-HTTP transport metadata namespace (intentionally distinct from legacy x-pipeline-*). */
    /** Header name for immutable end-to-end correlation id. */
    public static final String TPF_CORRELATION_ID = "x-tpf-correlation-id";
    /** Header name for orchestrator execution instance id. */
    public static final String TPF_EXECUTION_ID = "x-tpf-execution-id";
    /** Header name for idempotency token used for deduplication. */
    public static final String TPF_IDEMPOTENCY_KEY = "x-tpf-idempotency-key";
    /** Header name for retry attempt count (0-based). */
    public static final String TPF_RETRY_ATTEMPT = "x-tpf-retry-attempt";
    /** Header name for absolute deadline timestamp in epoch milliseconds. */
    public static final String TPF_DEADLINE_EPOCH_MS = "x-tpf-deadline-epoch-ms";
    /** Header name for dispatch timestamp in epoch milliseconds. */
    public static final String TPF_DISPATCH_TS_EPOCH_MS = "x-tpf-dispatch-ts-epoch-ms";
    /** Header name for optional parent lineage item identifier. */
    public static final String TPF_PARENT_ITEM_ID = "x-tpf-parent-item-id";

    private PipelineContextHeaders() {
    }
}
