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

package org.pipelineframework.telemetry;

import java.util.concurrent.TimeUnit;

/**
 * Best-effort flush for OpenTelemetry metrics/traces for short-lived CLI runs.
 */
public final class TelemetryFlush {

    private TelemetryFlush() {
    }

    /**
     * Compatibility delegate. SDK lookup remains owned by the production telemetry runtime.
     */
    public static void flush() {
        TelemetryCompatibilityAccess.runtime().flush();
    }

    static void flushSdk(Object openTelemetry) {
        try {
            Class<?> sdkClass = Class.forName("io.opentelemetry.sdk.OpenTelemetrySdk");
            if (!sdkClass.isInstance(openTelemetry)) {
                return;
            }
            Object sdk = sdkClass.cast(openTelemetry);
            Object meterProvider = sdkClass.getMethod("getSdkMeterProvider").invoke(sdk);
            if (meterProvider != null) {
                Object result = meterProvider.getClass().getMethod("forceFlush").invoke(meterProvider);
                waitForCompletion(result);
            }
            Object tracerProvider = sdkClass.getMethod("getSdkTracerProvider").invoke(sdk);
            if (tracerProvider != null) {
                Object result = tracerProvider.getClass().getMethod("forceFlush").invoke(tracerProvider);
                waitForCompletion(result);
            }
        } catch (Exception ignored) {
            // Best effort only; no logging to keep CLI output clean.
        }
    }

    private static void waitForCompletion(Object result) {
        if (result == null) {
            return;
        }
        try {
            Class<?> resultClass = Class.forName("io.opentelemetry.sdk.common.CompletableResultCode");
            if (resultClass.isInstance(result)) {
                resultClass.getMethod("join", long.class, TimeUnit.class).invoke(result, 5L, TimeUnit.SECONDS);
            }
        } catch (Exception ignored) {
            // Best effort only.
        }
    }
}
