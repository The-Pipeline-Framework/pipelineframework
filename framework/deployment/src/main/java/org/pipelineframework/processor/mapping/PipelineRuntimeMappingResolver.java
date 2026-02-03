package org.pipelineframework.processor.mapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;

import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.util.OrchestratorClientNaming;

/**
 * Resolves runtime mapping placement for pipeline steps and synthetic side effects.
 */
public class PipelineRuntimeMappingResolver {

    private static final String DEFAULT_MONOLITH_MODULE = "monolith";

    private final PipelineRuntimeMapping mapping;
    private final ProcessingEnvironment processingEnv;

    public PipelineRuntimeMappingResolver(PipelineRuntimeMapping mapping, ProcessingEnvironment processingEnv) {
        this.mapping = mapping;
        this.processingEnv = processingEnv;
    }

    public PipelineRuntimeMappingResolution resolve(List<PipelineStepModel> models) {
        if (mapping == null || models == null) {
            return new PipelineRuntimeMappingResolution(Map.of(), Map.of(), Map.of(), List.of(), List.of(), Set.of());
        }

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        Map<String, String> normalizedStepMappings = normalizeMappings(mapping.steps());
        Map<String, String> normalizedSyntheticMappings = normalizeMappings(mapping.synthetics());

        Map<String, String> moduleRuntimes = new LinkedHashMap<>(mapping.modules());
        validateRuntimeNames(moduleRuntimes, mapping.runtimes(), errors);

        Map<PipelineStepModel, String> moduleAssignments = new LinkedHashMap<>();
        Map<String, String> moduleByServicePackage = new LinkedHashMap<>();
        Map<String, String> clientOverrides = new LinkedHashMap<>();
        Set<String> usedModules = new LinkedHashSet<>();

        if (mapping.layout() == PipelineRuntimeMapping.Layout.MONOLITH) {
            String monolithModule = resolveMonolithModule(moduleRuntimes, mapping.defaults(), errors);
            for (PipelineStepModel model : models) {
                moduleAssignments.put(model, monolithModule);
                usedModules.add(monolithModule);
                if (model.servicePackage() != null) {
                    moduleByServicePackage.put(model.servicePackage(), monolithModule);
                }
                String clientName = OrchestratorClientNaming.clientNameForModel(model);
                if (clientName != null && !clientName.isBlank()) {
                    clientOverrides.put(clientName.toLowerCase(Locale.ROOT), monolithModule);
                }
            }
            if (mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                validateMonolithOverrides(mapping.steps(), mapping.synthetics(), monolithModule, errors);
            }
            return finish(moduleAssignments, clientOverrides, moduleByServicePackage, errors, warnings, usedModules);
        }

        List<PipelineStepModel> regularSteps = models.stream()
            .filter(model -> !model.sideEffect())
            .toList();
        List<PipelineStepModel> syntheticSteps = models.stream()
            .filter(PipelineStepModel::sideEffect)
            .toList();

        for (PipelineStepModel model : regularSteps) {
            String moduleName = resolveStepModule(model, normalizedStepMappings, moduleRuntimes, errors);
            if (moduleName != null && !moduleName.isBlank()) {
                moduleAssignments.put(model, moduleName);
                usedModules.add(moduleName);
                if (model.servicePackage() != null) {
                    moduleByServicePackage.put(model.servicePackage(), moduleName);
                }
                String clientName = OrchestratorClientNaming.clientNameForModel(model);
                if (clientName != null && !clientName.isBlank()) {
                    clientOverrides.put(clientName.toLowerCase(Locale.ROOT), moduleName);
                }
            } else if (mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                errors.add("Missing module placement for step '" + model.serviceName() + "'");
            }
        }

        SyntheticIndex syntheticIndex = buildSyntheticIndex(syntheticSteps);

        for (PipelineStepModel model : syntheticSteps) {
            String moduleName = resolveSyntheticModule(model, syntheticIndex, normalizedSyntheticMappings, moduleByServicePackage,
                moduleRuntimes, errors, warnings);
            if (moduleName != null && !moduleName.isBlank()) {
                moduleAssignments.put(model, moduleName);
                usedModules.add(moduleName);
                String clientName = OrchestratorClientNaming.clientNameForModel(model);
                if (clientName != null && !clientName.isBlank()) {
                    clientOverrides.put(clientName.toLowerCase(Locale.ROOT), moduleName);
                }
            } else if (mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                errors.add("Missing module placement for synthetic '" + model.serviceName() + "'");
            }
        }

        if (mapping.layout() == PipelineRuntimeMapping.Layout.PIPELINE_RUNTIME) {
            validateSingleRuntime(moduleAssignments, moduleRuntimes, errors, warnings);
        }

        return finish(moduleAssignments, clientOverrides, moduleByServicePackage, errors, warnings, usedModules);
    }

    private PipelineRuntimeMappingResolution finish(
        Map<PipelineStepModel, String> moduleAssignments,
        Map<String, String> clientOverrides,
        Map<String, String> moduleByServicePackage,
        List<String> errors,
        List<String> warnings,
        Set<String> usedModules
    ) {
        if (processingEnv != null) {
            var messager = processingEnv.getMessager();
            for (String warning : warnings) {
                messager.printMessage(javax.tools.Diagnostic.Kind.WARNING, warning);
            }
            for (String error : errors) {
                javax.tools.Diagnostic.Kind kind = mapping.validation() == PipelineRuntimeMapping.Validation.STRICT
                    ? javax.tools.Diagnostic.Kind.ERROR
                    : javax.tools.Diagnostic.Kind.WARNING;
                messager.printMessage(kind, error);
            }
        }
        if (mapping.validation() == PipelineRuntimeMapping.Validation.STRICT && !errors.isEmpty()) {
            throw new IllegalStateException(errors.get(0));
        }
        return new PipelineRuntimeMappingResolution(
            Map.copyOf(moduleAssignments),
            Map.copyOf(clientOverrides),
            Map.copyOf(moduleByServicePackage),
            List.copyOf(errors),
            List.copyOf(warnings),
            Set.copyOf(usedModules)
        );
    }

    private String resolveStepModule(
        PipelineStepModel model,
        Map<String, String> normalizedStepMappings,
        Map<String, String> moduleRuntimes,
        List<String> errors
    ) {
        String explicit = findStepMapping(model, normalizedStepMappings);
        if (explicit != null) {
            if (!moduleRuntimes.isEmpty() && !moduleRuntimes.containsKey(explicit)) {
                errors.add("Unknown module '" + explicit + "' for step '" + model.serviceName() + "'");
            }
            ensureModuleRuntime(moduleRuntimes, explicit);
            return explicit;
        }

        String defaultModule = mapping.defaults().module();
        if ("shared".equalsIgnoreCase(defaultModule)) {
            String sharedModule = resolveSharedModule(moduleRuntimes, errors);
            if (sharedModule != null) {
                ensureModuleRuntime(moduleRuntimes, sharedModule);
            }
            return sharedModule;
        }
        if ("per-step".equalsIgnoreCase(defaultModule)) {
            String name = defaultModuleForStep(model, errors);
            ensureModuleRuntime(moduleRuntimes, name);
            return name;
        }
        ensureModuleRuntime(moduleRuntimes, defaultModule);
        return defaultModule;
    }

    private String resolveSyntheticModule(
        PipelineStepModel model,
        SyntheticIndex syntheticIndex,
        Map<String, String> normalizedSyntheticMappings,
        Map<String, String> moduleByServicePackage,
        Map<String, String> moduleRuntimes,
        List<String> errors,
        List<String> warnings
    ) {
        SyntheticKey key = syntheticIndex.keyFor(model);
        if (key != null) {
            String explicit = resolveSyntheticMapping(key, normalizedSyntheticMappings, errors);
            if (explicit != null) {
                if (!moduleRuntimes.isEmpty() && !moduleRuntimes.containsKey(explicit)) {
                    errors.add("Unknown module '" + explicit + "' for synthetic '" + model.serviceName() + "'");
                }
                ensureModuleRuntime(moduleRuntimes, explicit);
                return explicit;
            }
        }

        String syntheticDefault = mapping.defaults().synthetic().module();
        if ("per-step".equalsIgnoreCase(syntheticDefault)) {
            String byPackage = moduleByServicePackage.get(model.servicePackage());
            if (byPackage != null && !byPackage.isBlank()) {
                ensureModuleRuntime(moduleRuntimes, byPackage);
                return byPackage;
            }
            warnings.add("Synthetic '" + model.serviceName()
                + "' did not match a step module; falling back to plugin default.");
            String name = defaultModuleForSideEffect(model);
            ensureModuleRuntime(moduleRuntimes, name);
            return name;
        }

        if ("plugin".equalsIgnoreCase(syntheticDefault)) {
            String name = defaultModuleForSideEffect(model);
            ensureModuleRuntime(moduleRuntimes, name);
            return name;
        }

        ensureModuleRuntime(moduleRuntimes, syntheticDefault);
        return syntheticDefault;
    }

    private void validateRuntimeNames(
        Map<String, String> modules,
        Map<String, String> runtimes,
        List<String> errors
    ) {
        if (runtimes == null || runtimes.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : modules.entrySet()) {
            String runtime = entry.getValue();
            if (runtime != null && !runtime.isBlank() && !runtimes.containsKey(runtime)) {
                errors.add("Unknown runtime '" + runtime + "' for module '" + entry.getKey() + "'");
            }
        }
    }

    private void validateSingleRuntime(
        Map<PipelineStepModel, String> moduleAssignments,
        Map<String, String> moduleRuntimes,
        List<String> errors,
        List<String> warnings
    ) {
        Set<String> runtimes = new LinkedHashSet<>();
        for (Map.Entry<PipelineStepModel, String> entry : moduleAssignments.entrySet()) {
            PipelineStepModel model = entry.getKey();
            if (model == null || model.sideEffect()) {
                continue;
            }
            String moduleName = entry.getValue();
            String runtime = moduleRuntimes.get(moduleName);
            if (runtime != null && !runtime.isBlank()) {
                runtimes.add(runtime);
            }
        }
        if (runtimes.size() > 1) {
            String message = "pipeline-runtime layout requires a single runtime per pipeline; found " + runtimes;
            if (mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                errors.add(message);
            } else {
                warnings.add(message);
            }
        }
    }

    private void validateMonolithOverrides(
        Map<String, String> stepMappings,
        Map<String, String> syntheticMappings,
        String monolithModule,
        List<String> errors
    ) {
        if (stepMappings != null) {
            for (Map.Entry<String, String> entry : stepMappings.entrySet()) {
                if (!monolithModule.equals(entry.getValue())) {
                    errors.add("Monolith layout requires all step mappings to target '" + monolithModule + "'");
                    return;
                }
            }
        }
        if (syntheticMappings != null) {
            for (Map.Entry<String, String> entry : syntheticMappings.entrySet()) {
                if (!monolithModule.equals(entry.getValue())) {
                    errors.add("Monolith layout requires all synthetic mappings to target '" + monolithModule + "'");
                    return;
                }
            }
        }
    }

    private String resolveMonolithModule(
        Map<String, String> moduleRuntimes,
        PipelineRuntimeMapping.Defaults defaults,
        List<String> errors
    ) {
        String defaultModule = defaults.module();
        if (defaultModule != null
            && !defaultModule.isBlank()
            && !"per-step".equalsIgnoreCase(defaultModule)
            && !"shared".equalsIgnoreCase(defaultModule)) {
            ensureModuleRuntime(moduleRuntimes, defaultModule);
            return defaultModule;
        }
        if (!moduleRuntimes.isEmpty()) {
            return moduleRuntimes.keySet().iterator().next();
        }
        String fallback = DEFAULT_MONOLITH_MODULE;
        ensureModuleRuntime(moduleRuntimes, fallback);
        return fallback;
    }

    private String resolveSharedModule(
        Map<String, String> moduleRuntimes,
        List<String> errors
    ) {
        if (moduleRuntimes.size() == 1) {
            return moduleRuntimes.keySet().iterator().next();
        }
        if (moduleRuntimes.isEmpty()) {
            errors.add("defaults.module=shared requires a declared module");
        } else {
            errors.add("defaults.module=shared requires exactly one module");
        }
        return null;
    }

    private String resolveSyntheticMapping(
        SyntheticKey key,
        Map<String, String> normalizedSyntheticMappings,
        List<String> errors
    ) {
        if (key == null) {
            return null;
        }
        String indexKey = key.payloadId() + "@" + key.index();
        String byIndex = normalizedSyntheticMappings.get(indexKey);
        if (byIndex != null) {
            return byIndex;
        }
        if (key.altPayloadId() != null && !key.altPayloadId().isBlank()) {
            String altIndexKey = key.altPayloadId() + "@" + key.index();
            String altByIndex = normalizedSyntheticMappings.get(altIndexKey);
            if (altByIndex != null) {
                return altByIndex;
            }
        }
        String payload = normalizedSyntheticMappings.get(key.payloadId());
        if (payload != null) {
            if (key.ambiguous() && mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                errors.add("Synthetic mapping '" + key.payloadId() + "' is ambiguous; use @<index>");
            }
            return payload;
        }
        if (key.altPayloadId() != null && !key.altPayloadId().isBlank()) {
            String altPayload = normalizedSyntheticMappings.get(key.altPayloadId());
            if (altPayload != null) {
                if (key.ambiguous() && mapping.validation() == PipelineRuntimeMapping.Validation.STRICT) {
                    errors.add("Synthetic mapping '" + key.altPayloadId() + "' is ambiguous; use @<index>");
                }
                return altPayload;
            }
        }
        String generic = normalizedSyntheticMappings.get(key.genericId());
        if (generic != null) {
            return generic;
        }
        if (key.altGenericId() != null && !key.altGenericId().isBlank()) {
            String altGeneric = normalizedSyntheticMappings.get(key.altGenericId());
            if (altGeneric != null) {
                return altGeneric;
            }
        }
        return null;
    }

    private String findStepMapping(PipelineStepModel model, Map<String, String> normalizedMappings) {
        if (model == null || normalizedMappings.isEmpty()) {
            return null;
        }
        List<String> candidates = stepCandidates(model);
        for (String candidate : candidates) {
            String module = normalizedMappings.get(candidate);
            if (module != null) {
                return module;
            }
        }
        return null;
    }

    private List<String> stepCandidates(PipelineStepModel model) {
        List<String> candidates = new ArrayList<>();
        String serviceName = safeLower(model.serviceName());
        String baseName = safeLower(OrchestratorClientNaming.baseServiceName(model.serviceName()));
        String clientName = safeLower(OrchestratorClientNaming.clientNameForModel(model));
        if (!serviceName.isBlank()) {
            candidates.add(serviceName);
        }
        if (!baseName.isBlank()) {
            candidates.add(baseName);
        }
        if (!clientName.isBlank()) {
            candidates.add(clientName);
        }
        String kebab = safeLower(OrchestratorClientNaming.toKebabCase(baseName));
        if (!kebab.isBlank()) {
            candidates.add(kebab);
        }
        return candidates;
    }

    private Map<String, String> normalizeMappings(Map<String, String> mapping) {
        Map<String, String> normalized = new LinkedHashMap<>();
        if (mapping == null) {
            return normalized;
        }
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            String key = safeLower(entry.getKey());
            String module = entry.getValue();
            if (key.isBlank() || module == null || module.isBlank()) {
                continue;
            }
            normalized.put(key, module);
        }
        return normalized;
    }

    private void ensureModuleRuntime(Map<String, String> moduleRuntimes, String moduleName) {
        if (moduleName == null || moduleName.isBlank()) {
            return;
        }
        moduleRuntimes.putIfAbsent(moduleName, mapping.defaults().runtime());
    }

    private String defaultModuleForStep(PipelineStepModel model, List<String> errors) {
        String baseName = OrchestratorClientNaming.baseServiceName(model.serviceName());
        if (baseName.isBlank()) {
            errors.add("Unable to derive module name for step '" + model.serviceName() + "'");
            return null;
        }
        return OrchestratorClientNaming.toKebabCase(baseName) + "-svc";
    }

    private String defaultModuleForSideEffect(PipelineStepModel model) {
        String aspectName = OrchestratorClientNaming.resolveAspectName(model);
        if (aspectName.startsWith("persistence")) {
            return "persistence-svc";
        }
        if (aspectName.startsWith("cache")) {
            return "cache-invalidation-svc";
        }
        return aspectName + "-svc";
    }

    private static String safeLower(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private SyntheticIndex buildSyntheticIndex(List<PipelineStepModel> syntheticSteps) {
        Map<String, List<PipelineStepModel>> payloadIndex = new LinkedHashMap<>();
        for (PipelineStepModel model : syntheticSteps) {
            SyntheticKey key = SyntheticKey.fromModel(model);
            if (key == null) {
                continue;
            }
            payloadIndex.computeIfAbsent(key.payloadId(), k -> new ArrayList<>()).add(model);
        }
        return new SyntheticIndex(payloadIndex);
    }

    private record SyntheticIndex(Map<String, List<PipelineStepModel>> payloadIndex) {
        SyntheticKey keyFor(PipelineStepModel model) {
            SyntheticKey base = SyntheticKey.fromModel(model);
            if (base == null) {
                return null;
            }
            List<PipelineStepModel> list = payloadIndex.get(base.payloadId());
            int index = list == null ? 0 : list.indexOf(model);
            boolean ambiguous = list != null && list.size() > 1;
            return base.withIndex(index, ambiguous);
        }
    }

    private record SyntheticKey(String payloadId,
                                String genericId,
                                String altPayloadId,
                                String altGenericId,
                                int index,
                                boolean ambiguous) {
        static SyntheticKey fromModel(PipelineStepModel model) {
            if (model == null) {
                return null;
            }
            String aspectId = resolveAspectId(model);
            String payloadType = OrchestratorClientNaming.resolveSideEffectTypeName(model);
            if (aspectId.isBlank() || payloadType == null || payloadType.isBlank()) {
                return null;
            }
            String payloadId = safeLower(aspectId + "." + payloadType);
            String genericId = safeLower(aspectId + ".SideEffect");
            String altAspect = aspectId.startsWith("Observe")
                ? aspectId.substring("Observe".length())
                : "";
            String altPayloadId = altAspect.isBlank()
                ? ""
                : safeLower(altAspect + "." + payloadType);
            String altGenericId = altAspect.isBlank()
                ? ""
                : safeLower(altAspect + ".SideEffect");
            return new SyntheticKey(payloadId, genericId, altPayloadId, altGenericId, 0, false);
        }

        SyntheticKey withIndex(int index, boolean ambiguous) {
            return new SyntheticKey(payloadId, genericId, altPayloadId, altGenericId, index, ambiguous);
        }

        private static String resolveAspectId(PipelineStepModel model) {
            String serviceName = model.serviceName();
            if (serviceName == null) {
                return "";
            }
            String typeName = OrchestratorClientNaming.resolveSideEffectTypeName(model);
            String trimmed = serviceName;
            if (trimmed.endsWith("SideEffectService")) {
                trimmed = trimmed.substring(0, trimmed.length() - "SideEffectService".length());
            }
            if (typeName != null && !typeName.isBlank() && trimmed.endsWith(typeName)) {
                trimmed = trimmed.substring(0, trimmed.length() - typeName.length());
            }
            return trimmed;
        }

    }
}
