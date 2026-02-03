package org.pipelineframework.processor.mapping;

import java.util.Locale;
import java.util.Map;

/**
 * Parsed runtime mapping configuration from pipeline.runtime.yaml.
 *
 * @param layout layout mode (modular, pipeline-runtime, monolith)
 * @param validation validation mode (auto, strict)
 * @param defaults default placement behavior
 * @param runtimes runtime names declared in the mapping
 * @param modules module-to-runtime mapping
 * @param steps explicit step-to-module mapping
 * @param synthetics explicit synthetic-to-module mapping
 */
public record PipelineRuntimeMapping(
    Layout layout,
    Validation validation,
    Defaults defaults,
    Map<String, String> runtimes,
    Map<String, String> modules,
    Map<String, String> steps,
    Map<String, String> synthetics
) {
    public PipelineRuntimeMapping {
        layout = layout == null ? Layout.MODULAR : layout;
        validation = validation == null ? Validation.AUTO : validation;
        defaults = defaults == null ? Defaults.defaultValues() : defaults;
        runtimes = runtimes == null ? Map.of() : Map.copyOf(runtimes);
        modules = modules == null ? Map.of() : Map.copyOf(modules);
        steps = steps == null ? Map.of() : Map.copyOf(steps);
        synthetics = synthetics == null ? Map.of() : Map.copyOf(synthetics);
    }

    public enum Layout {
        MODULAR,
        PIPELINE_RUNTIME,
        MONOLITH;

        public static Layout fromString(String value) {
            if (value == null || value.isBlank()) {
                return MODULAR;
            }
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            return switch (normalized) {
                case "pipeline-runtime", "pipeline_runtime", "pipeline" -> PIPELINE_RUNTIME;
                case "monolith" -> MONOLITH;
                default -> MODULAR;
            };
        }
    }

    public enum Validation {
        AUTO,
        STRICT;

        public static Validation fromString(String value) {
            if (value == null || value.isBlank()) {
                return AUTO;
            }
            String normalized = value.trim().toLowerCase(Locale.ROOT);
            if ("strict".equals(normalized)) {
                return STRICT;
            }
            return AUTO;
        }
    }

    public record Defaults(String runtime, String module, SyntheticDefaults synthetic) {
        public Defaults {
            runtime = runtime == null || runtime.isBlank() ? "local" : runtime;
            module = module == null || module.isBlank() ? "per-step" : module;
            synthetic = synthetic == null ? SyntheticDefaults.defaultValues() : synthetic;
        }

        public static Defaults defaultValues() {
            return new Defaults("local", "per-step", SyntheticDefaults.defaultValues());
        }
    }

    public record SyntheticDefaults(String module) {
        public SyntheticDefaults {
            module = module == null || module.isBlank() ? "plugin" : module;
        }

        public static SyntheticDefaults defaultValues() {
            return new SyntheticDefaults("plugin");
        }
    }
}
