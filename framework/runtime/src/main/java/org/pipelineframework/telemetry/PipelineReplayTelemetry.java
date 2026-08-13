/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.telemetry;

import java.util.Map;

/** Focused replay-observation seam for runtime boundaries. */
public interface PipelineReplayTelemetry {
    void recordAwaitLifecycle(AwaitReplayLifecycleEvent event);
    void recordConnectorReplayEvent(
        String connectorStep, String service, String eventName, String from, String to,
        Map<String, String> attributes);
}
