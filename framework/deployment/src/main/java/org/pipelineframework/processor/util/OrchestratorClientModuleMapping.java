package org.pipelineframework.processor.util;

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;

import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Resolves orchestrator client module mappings from application.properties.
 */
public class OrchestratorClientModuleMapping {

    private static final String MODULE_PREFIX = "pipeline.module.";
    private static final String BASE_PORT_KEY = "pipeline.client.base-port";
    private static final String TLS_CONFIG_KEY = "pipeline.client.tls-configuration-name";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_BASE_PORT = 8443;

    private final Map<String, ModuleConfig> modules;
    private final Map<String, String> stepToModule;
    private final Map<String, String> aspectToModule;
    private final int basePort;
    private final String tlsConfigurationName;

    private OrchestratorClientModuleMapping(
        Map<String, ModuleConfig> modules,
        Map<String, String> stepToModule,
        Map<String, String> aspectToModule,
        int basePort,
        String tlsConfigurationName
    ) {
        this.modules = modules;
        this.stepToModule = stepToModule;
        this.aspectToModule = aspectToModule;
        this.basePort = basePort;
        this.tlsConfigurationName = tlsConfigurationName;
    }

    public static OrchestratorClientModuleMapping fromProperties(Properties properties, ProcessingEnvironment env) {
        Map<String, ModuleConfig> modules = new LinkedHashMap<>();
        Map<String, String> stepToModule = new LinkedHashMap<>();
        Map<String, String> aspectToModule = new LinkedHashMap<>();
        int basePort = DEFAULT_BASE_PORT;
        String tlsConfigurationName = null;

        if (properties != null) {
            String basePortValue = properties.getProperty(BASE_PORT_KEY);
            if (basePortValue != null && !basePortValue.isBlank()) {
                try {
                    basePort = Integer.parseInt(basePortValue.trim());
                } catch (NumberFormatException e) {
                    env.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                        "Invalid pipeline.module.base-port value '" + basePortValue + "': " + e.getMessage());
                }
            }

            String tlsConfigValue = properties.getProperty(TLS_CONFIG_KEY);
            if (tlsConfigValue != null && !tlsConfigValue.isBlank()) {
                tlsConfigurationName = tlsConfigValue.trim();
            }

            for (String key : properties.stringPropertyNames()) {
                if (!key.startsWith(MODULE_PREFIX) || key.equals(BASE_PORT_KEY)) {
                    continue;
                }
                String remainder = key.substring(MODULE_PREFIX.length());
                int lastDot = remainder.lastIndexOf('.');
                if (lastDot <= 0 || lastDot == remainder.length() - 1) {
                    continue;
                }
                String moduleName = remainder.substring(0, lastDot).trim();
                String field = remainder.substring(lastDot + 1).trim();
                if (moduleName.isBlank() || field.isBlank()) {
                    continue;
                }
                ModuleConfig existing = modules.getOrDefault(moduleName, new ModuleConfig(moduleName));
                String rawValue = properties.getProperty(key, "").trim();
                switch (field) {
                    case "host" -> modules.put(moduleName, new ModuleConfig(moduleName, rawValue, existing.port()));
                    case "port" -> {
                        Integer port = parsePort(rawValue, moduleName, env);
                        modules.put(moduleName, new ModuleConfig(moduleName, existing.host(), port));
                    }
                    case "steps" -> {
                        List<String> steps = splitList(rawValue);
                        for (String step : steps) {
                            String normalized = normalizeClientToken(step);
                            if (normalized.isBlank()) {
                                continue;
                            }
                            registerMapping(stepToModule, normalized, moduleName, env, "step");
                        }
                    }
                    case "aspects" -> {
                        List<String> aspects = splitList(rawValue);
                        for (String aspect : aspects) {
                            String normalized = aspect.trim().toLowerCase(Locale.ROOT);
                            if (normalized.isBlank()) {
                                continue;
                            }
                            registerMapping(aspectToModule, normalized, moduleName, env, "aspect");
                        }
                    }
                    default -> {
                        // Ignore unknown fields.
                    }
                }
            }
        }

        return new OrchestratorClientModuleMapping(
            modules,
            stepToModule,
            aspectToModule,
            basePort,
            tlsConfigurationName
        );
    }

    public OrchestratorClientModuleMapping withResolvedModules(List<String> moduleOrder) {
        Map<String, ModuleConfig> resolved = new LinkedHashMap<>(modules);
        for (int i = 0; i < moduleOrder.size(); i++) {
            String name = moduleOrder.get(i);
            ModuleConfig config = resolved.getOrDefault(name, new ModuleConfig(name));
            int port = config.port() != null ? config.port() : basePort + i + 1;
            String host = config.host() != null && !config.host().isBlank() ? config.host() : DEFAULT_HOST;
            resolved.put(name, new ModuleConfig(name, host, port));
        }
        return new OrchestratorClientModuleMapping(resolved, stepToModule, aspectToModule, basePort, tlsConfigurationName);
    }

    public String resolveModuleName(PipelineStepModel model) {
        String clientName = OrchestratorClientNaming.clientNameForModel(model);
        if (clientName != null) {
            String override = stepToModule.get(clientName);
            if (override != null) {
                return override;
            }
        }
        if (model.sideEffect()) {
            String aspectName = OrchestratorClientNaming.resolveAspectName(model);
            String aspectModule = aspectToModule.get(aspectName);
            if (aspectModule != null) {
                return aspectModule;
            }
            if (aspectName.startsWith("persistence")) {
                return "persistence-svc";
            }
            if (aspectName.startsWith("cache")) {
                return "cache-invalidation-svc";
            }
            return aspectName + "-svc";
        }

        String baseName = OrchestratorClientNaming.baseServiceName(model.serviceName());
        if (baseName.isBlank()) {
            return null;
        }
        return OrchestratorClientNaming.toKebabCase(baseName) + "-svc";
    }

    public ClientConfig clientConfig(PipelineStepModel model) {
        String clientName = OrchestratorClientNaming.clientNameForModel(model);
        if (clientName == null) {
            return null;
        }
        String moduleName = resolveModuleName(model);
        ModuleConfig module = modules.get(moduleName);
        if (module == null) {
            return null;
        }
        return new ClientConfig(clientName, module.host(), module.port(), tlsConfigurationName);
    }

    private static Integer parsePort(String value, String moduleName, ProcessingEnvironment env) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            env.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                "Invalid port for module '" + moduleName + "': " + e.getMessage());
            return null;
        }
    }

    private static List<String> splitList(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return List.of();
        }
        return Arrays.stream(rawValue.split("[,\\s]+"))
            .map(String::trim)
            .filter(token -> !token.isBlank())
            .collect(Collectors.toList());
    }

    private static String normalizeClientToken(String token) {
        if (token == null) {
            return "";
        }
        String trimmed = token.trim();
        if (trimmed.isBlank()) {
            return "";
        }
        if (trimmed.startsWith("process-") || trimmed.startsWith("observe-")) {
            return trimmed.toLowerCase(Locale.ROOT);
        }
        if (trimmed.startsWith("Process") && trimmed.endsWith("Service")) {
            String base = trimmed.substring("Process".length(), trimmed.length() - "Service".length());
            return "process-" + OrchestratorClientNaming.toKebabCase(base);
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    private static void registerMapping(
        Map<String, String> mapping,
        String key,
        String moduleName,
        ProcessingEnvironment env,
        String kind
    ) {
        String existing = mapping.get(key);
        if (existing != null && !existing.equals(moduleName)) {
            env.getMessager().printMessage(javax.tools.Diagnostic.Kind.WARNING,
                "Ignoring duplicate " + kind + " mapping for '" + key + "'; already mapped to '" + existing + "'");
            return;
        }
        mapping.put(key, moduleName);
    }

    public record ModuleConfig(String name, String host, Integer port) {
        public ModuleConfig(String name) {
            this(name, null, null);
        }
    }

    public record ClientConfig(String name, String host, int port, String tlsConfigurationName) {
    }
}
