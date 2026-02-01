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

package org.pipelineframework.config;

import java.util.Locale;

import io.smallrye.config.Converter;
import org.pipelineframework.telemetry.RetryAmplificationGuardMode;

/**
 * Converts retry amplification guard mode strings to enum values.
 */
public final class RetryAmplificationGuardModeConverter implements Converter<RetryAmplificationGuardMode> {

    @Override
    public RetryAmplificationGuardMode convert(String value) {
        if (value == null || value.isBlank()) {
            return RetryAmplificationGuardMode.FAIL_FAST;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "fail-fast", "fail_fast", "failfast", "fail" -> RetryAmplificationGuardMode.FAIL_FAST;
            case "log-only", "log_only", "logonly", "log" -> RetryAmplificationGuardMode.LOG_ONLY;
            default -> throw new IllegalArgumentException("Unsupported retry amplification guard mode: " + value);
        };
    }
}
