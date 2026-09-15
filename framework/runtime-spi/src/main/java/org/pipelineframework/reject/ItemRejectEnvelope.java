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

import java.util.Objects;

/**
 * Envelope published by item reject sink providers.
 *
 * @param tenantId tenant identifier when available
 * @param executionId execution identifier when available
 * @param correlationId correlation identifier when available
 * @param idempotencyKey idempotency key when available
 * @param replayMode replay mode when available
 * @param stepClass fully qualified step class name
 * @param stepName simple step class name
 * @param rejectScope ITEM or STREAM scope
 * @param transportRetryAttempt transport retry attempt when available
 * @param retriesObserved retries observed in the step execution path
 * @param retryLimit configured retry limit for the step
 * @param finalAttempt computed final attempt number
 * @param errorClass failure class name
 * @param errorMessage failure message
 * @param rejectedAtEpochMs reject timestamp epoch milliseconds
 * @param itemFingerprint deterministic item fingerprint
 * @param itemCount item count when rejectScope is STREAM
 * @param payload payload only when include-payload is enabled
 */
public record ItemRejectEnvelope(
    String tenantId,
    String executionId,
    String correlationId,
    String idempotencyKey,
    String replayMode,
    String stepClass,
    String stepName,
    String rejectScope,
    Integer transportRetryAttempt,
    Integer retriesObserved,
    Integer retryLimit,
    Integer finalAttempt,
    String errorClass,
    String errorMessage,
    long rejectedAtEpochMs,
    String itemFingerprint,
    Long itemCount,
    Object payload
) {

    public ItemRejectEnvelope {
        stepClass = Objects.requireNonNull(stepClass, "ItemRejectEnvelope.stepClass must not be null");
        stepName = Objects.requireNonNull(stepName, "ItemRejectEnvelope.stepName must not be null");
        rejectScope = Objects.requireNonNull(rejectScope, "ItemRejectEnvelope.rejectScope must not be null");
        errorClass = Objects.requireNonNull(errorClass, "ItemRejectEnvelope.errorClass must not be null");
        itemFingerprint = Objects.requireNonNull(itemFingerprint, "ItemRejectEnvelope.itemFingerprint must not be null");
        if (rejectedAtEpochMs <= 0) {
            throw new IllegalArgumentException("ItemRejectEnvelope.rejectedAtEpochMs must be > 0");
        }
    }
}
