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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class AdapterUtils {
    private static final String KEY_FUNCTION_NAME = "functionName";
    private static final String KEY_STAGE = "stage";
    private static final String KEY_REQUEST_ID = "requestId";
    private static final Set<String> RESERVED_CONTEXT_META_KEYS = Set.of(KEY_FUNCTION_NAME, KEY_STAGE, KEY_REQUEST_ID);

    private AdapterUtils() {
    }

    static String normalizeOrDefault(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.strip();
        return trimmed.isEmpty() ? fallback : trimmed;
    }

    static String deriveTraceId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return UUID.randomUUID().toString();
        }
        return requestId.strip();
    }

    static Map<String, String> buildContextMeta(FunctionTransportContext context) {
        LinkedHashMap<String, String> meta = new LinkedHashMap<>();
        meta.put(KEY_FUNCTION_NAME, normalizeOrDefault(context.functionName(), ""));
        meta.put(KEY_STAGE, normalizeOrDefault(context.stage(), ""));
        meta.put(KEY_REQUEST_ID, normalizeOrDefault(context.requestId(), ""));
        if (context.attributes() != null && !context.attributes().isEmpty()) {
            context.attributes().forEach((key, value) -> {
                if (key != null
                        && !RESERVED_CONTEXT_META_KEYS.contains(key)
                        && !key.startsWith("tpf.")) {
                    meta.put(key, normalizeOrDefault(value, ""));
                }
            });
        }
        return meta;
    }
}
