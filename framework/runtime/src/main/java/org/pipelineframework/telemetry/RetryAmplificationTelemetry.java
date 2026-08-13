/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.Optional;

/** Focused access to retry-amplification safety state; independent of signal export. */
public interface RetryAmplificationTelemetry {
    boolean retryAmplificationGuardEnabled();
    RetryAmplificationGuardMode retryAmplificationMode();
    Duration retryAmplificationCheckInterval();
    Optional<RetryAmplificationGuard.Trigger> retryAmplificationTrigger(PipelineRunContext context);
}
