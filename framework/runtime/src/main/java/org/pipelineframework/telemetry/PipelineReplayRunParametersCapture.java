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

package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.pipelineframework.config.PipelineStepConfig;

final class PipelineReplayRunParametersCapture {

    private static final String ITEM_REJECT_PROVIDER_KEY = "pipeline.item-reject.provider";
    private static final List<String> STEP_OVERRIDE_SUFFIXES = List.of(
        "retry-limit",
        "retry-wait-ms",
        "max-backoff",
        "jitter",
        "recover-on-failure",
        "backpressure-buffer-capacity",
        "backpressure-strategy");

    private PipelineReplayRunParametersCapture() {
    }

    static PipelineReplayRunParameters capture(PipelineStepConfig stepConfig) {
        Config config = ConfigProvider.getConfig();
        List<PipelineReplayParameterSection> sections = new ArrayList<>();

        sections.add(section("execution", "Execution", List.of(
            entry("pipeline.parallelism", "Parallelism", enumValue(stepConfig.parallelism())),
            entry("pipeline.max-concurrency", "Max concurrency", String.valueOf(stepConfig.maxConcurrency()))
        )));

        PipelineStepConfig.StepConfig defaults = stepConfig.defaults();
        sections.add(section("step-defaults", "Step defaults", List.of(
            entry("pipeline.defaults.retry-limit", "Retry limit", String.valueOf(defaults.retryLimit())),
            entry("pipeline.defaults.retry-wait-ms", "Retry wait (ms)", String.valueOf(defaults.retryWaitMs())),
            entry("pipeline.defaults.max-backoff", "Max backoff (ms)", String.valueOf(defaults.maxBackoff())),
            entry("pipeline.defaults.jitter", "Jitter", String.valueOf(defaults.jitter())),
            entry("pipeline.defaults.recover-on-failure", "Recover on failure", String.valueOf(defaults.recoverOnFailure())),
            entry("pipeline.defaults.backpressure-buffer-capacity", "Backpressure buffer capacity", String.valueOf(defaults.backpressureBufferCapacity())),
            entry("pipeline.defaults.backpressure-strategy", "Backpressure strategy", defaults.backpressureStrategy())
        )));

        List<PipelineReplayParameterEntry> overrideEntries = collectStepOverrides(config);
        if (!overrideEntries.isEmpty()) {
            sections.add(section("step-overrides", "Step overrides", overrideEntries));
        }

        PipelineStepConfig.CacheConfig cache = stepConfig.cache();
        List<PipelineReplayParameterEntry> cacheEntries = new ArrayList<>();
        cache.provider().ifPresent(provider ->
            cacheEntries.add(entry("pipeline.cache.provider", "Cache provider", provider)));
        cacheEntries.add(entry("pipeline.cache.policy", "Cache policy", cache.policy()));
        cache.ttl().map(Duration::toString).ifPresent(ttl ->
            cacheEntries.add(entry("pipeline.cache.ttl", "Cache TTL", ttl)));
        if (!cacheEntries.isEmpty()) {
            sections.add(section("cache", "Cache", cacheEntries));
        }

        PipelineStepConfig.TelemetryConfig telemetry = stepConfig.telemetry();
        sections.add(section("telemetry", "Telemetry", List.of(
            entry("pipeline.telemetry.enabled", "Telemetry enabled", String.valueOf(telemetry.enabled())),
            entry("pipeline.telemetry.tracing.enabled", "Tracing enabled", String.valueOf(telemetry.tracing().enabled())),
            entry("pipeline.telemetry.tracing.per-item", "Per-item tracing", String.valueOf(telemetry.tracing().perItem())),
            entry("pipeline.telemetry.replay.enabled", "Replay enabled", String.valueOf(telemetry.replay().enabled())),
            entry("pipeline.telemetry.replay.exporter", "Replay exporter", telemetry.replay().exporter())
        )));

        PipelineStepConfig.RetryAmplificationGuardConfig guard = stepConfig.killSwitch().retryAmplification();
        sections.add(section("guardrails", "Guardrails", List.of(
            entry("pipeline.kill-switch.retry-amplification.enabled", "Retry amplification enabled", String.valueOf(guard.enabled())),
            entry("pipeline.kill-switch.retry-amplification.window", "Retry amplification window", guard.window().toString()),
            entry("pipeline.kill-switch.retry-amplification.inflight-slope-threshold", "Inflight slope threshold", String.valueOf(guard.inflightSlopeThreshold())),
            entry("pipeline.kill-switch.retry-amplification.sustain-samples", "Sustain samples", String.valueOf(guard.sustainSamples())),
            entry("pipeline.kill-switch.retry-amplification.mode", "Retry amplification mode", enumValue(guard.mode()))
        )));

        sections.add(section("reject-sink", "Reject sink", List.of(
            entry(
                ITEM_REJECT_PROVIDER_KEY,
                "Reject sink provider",
                config.getOptionalValue(ITEM_REJECT_PROVIDER_KEY, String.class).orElse("log"))
        )));

        return new PipelineReplayRunParameters(List.copyOf(sections));
    }

    private static List<PipelineReplayParameterEntry> collectStepOverrides(Config config) {
        Map<String, PipelineReplayParameterEntry> entries = new LinkedHashMap<>();
        for (String propertyName : config.getPropertyNames()) {
            if (propertyName == null || !propertyName.startsWith("pipeline.step.")) {
                continue;
            }
            if (STEP_OVERRIDE_SUFFIXES.stream().noneMatch(suffix -> propertyName.endsWith("." + suffix))) {
                continue;
            }
            String value = config.getOptionalValue(propertyName, String.class).orElse(null);
            if (value == null || value.isBlank()) {
                continue;
            }
            entries.put(propertyName, entry(propertyName, displayLabelForStepOverride(propertyName), value));
        }
        return entries.values().stream()
            .sorted(
                Comparator.comparing(PipelineReplayParameterEntry::label)
                    .thenComparing(PipelineReplayParameterEntry::key))
            .toList();
    }

    private static String displayLabelForStepOverride(String propertyName) {
        int firstQuote = propertyName.indexOf('"');
        int secondQuote = propertyName.indexOf('"', firstQuote + 1);
        if (firstQuote < 0 || secondQuote <= firstQuote) {
            return propertyName;
        }
        String stepClass = propertyName.substring(firstQuote + 1, secondQuote);
        String suffix = propertyName.substring(secondQuote + 2);
        int lastDot = stepClass.lastIndexOf('.');
        int previousDot = lastDot < 0 ? -1 : stepClass.lastIndexOf('.', lastDot - 1);
        String displayStep =
            previousDot >= 0 ? stepClass.substring(previousDot + 1) : stepClass.substring(lastDot + 1);
        return displayStep + "." + suffix;
    }

    private static PipelineReplayParameterSection section(
        String id,
        String label,
        List<PipelineReplayParameterEntry> entries
    ) {
        return new PipelineReplayParameterSection(id, label, List.copyOf(entries));
    }

    private static PipelineReplayParameterEntry entry(String key, String label, String value) {
        return new PipelineReplayParameterEntry(key, label, value);
    }

    private static String enumValue(Enum<?> value) {
        return value == null ? "" : value.name().toLowerCase(Locale.ROOT);
    }
}
