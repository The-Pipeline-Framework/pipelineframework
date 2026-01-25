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

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Resolves telemetry SLO thresholds from configuration.
 */
final class TelemetrySloConfig {

    private static final String RPC_LATENCY_MS = "pipeline.telemetry.slo.rpc-latency-ms";
    private static final String ITEM_THROUGHPUT_PER_MIN = "pipeline.telemetry.slo.item-throughput-per-min";
    private static final double DEFAULT_RPC_LATENCY_MS = 1000.0;
    private static final double DEFAULT_ITEM_THROUGHPUT_PER_MIN = 1000.0;

    private TelemetrySloConfig() {
    }

    static double rpcLatencyMs() {
        return readDouble(RPC_LATENCY_MS, DEFAULT_RPC_LATENCY_MS);
    }

    static double itemThroughputPerMinute() {
        return readDouble(ITEM_THROUGHPUT_PER_MIN, DEFAULT_ITEM_THROUGHPUT_PER_MIN);
    }

    private static double readDouble(String key, double fallback) {
        try {
            Config config = ConfigProvider.getConfig();
            return config.getOptionalValue(key, Double.class).orElse(fallback);
        } catch (Exception e) {
            return fallback;
        }
    }
}
