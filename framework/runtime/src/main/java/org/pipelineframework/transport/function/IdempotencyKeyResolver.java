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

final class IdempotencyKeyResolver {
    private IdempotencyKeyResolver() {
    }

    static String resolve(
            FunctionTransportContext context,
            String traceId,
            String payloadModel,
            String deterministicSuffix) {
        return resolve(context, traceId, payloadModel, deterministicSuffix, null);
    }

    static String resolve(
            FunctionTransportContext context,
            String traceId,
            String payloadModel,
            String deterministicSuffix,
            String explicitSuffix) {
        String normalizedTraceId = AdapterUtils.normalizeOrDefault(traceId, "trace");
        String normalizedPayloadModel = AdapterUtils.normalizeOrDefault(payloadModel, "unknown.payload");
        String normalizedSuffix = AdapterUtils.normalizeOrDefault(deterministicSuffix, "item");
        String fallbackKey = normalizedTraceId + ":" + normalizedPayloadModel + ":" + normalizedSuffix;

        if (context.idempotencyPolicy() == IdempotencyPolicy.EXPLICIT) {
            return context.explicitIdempotencyKey()
                .map(explicit -> {
                    String normalizedExplicit = AdapterUtils.normalizeOrDefault(explicit, "");
                    if (normalizedExplicit.isEmpty()) {
                        return fallbackKey;
                    }
                    String normalizedExplicitSuffix = AdapterUtils.normalizeOrDefault(explicitSuffix, "");
                    if (normalizedExplicitSuffix.isEmpty()) {
                        return normalizedExplicit;
                    }
                    return normalizedExplicit + ":" + normalizedExplicitSuffix;
                })
                .orElse(fallbackKey);
        }

        return fallbackKey;
    }
}
