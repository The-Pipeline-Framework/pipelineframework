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

package org.pipelineframework;

import org.pipelineframework.telemetry.RetryAmplificationGuard;

/**
 * Exception raised when a pipeline kill switch aborts execution.
 */
public class PipelineKillSwitchException extends RuntimeException {

    private final String reason;
    private final String step;

    public PipelineKillSwitchException(String message, String reason, String step) {
        super(message);
        this.reason = reason;
        this.step = step;
    }

    public String reason() {
        return reason;
    }

    public String step() {
        return step;
    }

    public static PipelineKillSwitchException retryAmplification(RetryAmplificationGuard.Trigger trigger) {
        String message = String.format(
            "Retry amplification guard triggered for step %s (inflight slope %.2f > %.2f, retry rate %.2f) over %s (sustain samples %d)",
            trigger.step(),
            trigger.inflightSlope(),
            trigger.inflightSlopeThreshold(),
            trigger.retryRate(),
            trigger.window(),
            trigger.sustainSamples());
        return new PipelineKillSwitchException(message, "retry_amplification", trigger.step());
    }
}
