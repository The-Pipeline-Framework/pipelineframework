/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.pipelineframework.command;

import org.pipelineframework.telemetry.TelemetryCompatibilityAccess;

/** Compatibility delegates for command-effect telemetry. */
public final class CommandEffectMetrics {
    static final String TRANSITION_TOTAL = "tpf.command.effect.transition.total";
    static final String DUPLICATE_TOTAL = "tpf.command.effect.duplicate.total";
    static final String DURATION = "tpf.command.effect.duration";
    static final String ADMISSION_TOTAL = "tpf.command.effect.admission.total";
    private CommandEffectMetrics() { }

    private static CommandEffectMetricsRecorder delegate() {
        return TelemetryCompatibilityAccess.adapter(CommandEffectMetricsRecorder.class, CommandEffectMetricsRecorder::new);
    }

    public static long startNanos() { return delegate().startNanos(); }

    public static void recordTransition(CommandDescriptor descriptor, CommandEffectStatus status) {
        delegate().recordTransition(descriptor, status);
    }

    public static void recordTerminalTransition(
        CommandDescriptor descriptor, CommandEffectStatus status, long startNanos
    ) {
        delegate().recordTerminalTransition(descriptor, status, startNanos);
    }

    public static void recordDuplicate(CommandDescriptor descriptor, String duplicateResult) {
        delegate().recordDuplicate(descriptor, duplicateResult);
    }

    public static void recordAdmission(CommandDescriptor descriptor, String admission) {
        delegate().recordAdmission(descriptor, admission);
    }
}
