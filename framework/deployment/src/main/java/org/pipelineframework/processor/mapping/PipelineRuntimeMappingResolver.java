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
    private static final String PERSISTENCE_ASPECT_PREFIX = "persistence";
    private static final String CACHE_ASPECT_PREFIX = "cache";
    private static final String PERSISTENCE_MODULE = "persistence-svc";
    private static final String CACHE_MODULE = "cache-invalidation-svc";

    private final PipelineRuntimeMapping mapping;
    private final ProcessingEnvironment processingEnv;

    /**
     * Create a resolver configured with the given runtime mapping and processing environment.
     *
     * The provided mapping drives module and runtime resolution behavior; the processing environment,
     * if non-null, is used to emit diagnostics (warnings/errors) during resolution.
     *
     * @param mapping         the PipelineRuntimeMapping describing mappings, defaults, layout, and validation; may be null
     * @param processingEnv   the ProcessingEnvironment used to report diagnostics; may be null
     */
    public PipelineRuntimeMappingResolver(PipelineRuntimeMapping mapping, ProcessingEnvironment processingEnv) {
        this.mapping = mapping;
        this.processingEnv = processingEnv;
    }

    /**
     * Resolve module placements and related mappings for the given pipeline step models using the configured PipelineRuntimeMapping.
     *
     * <p>Assigns each step (regular and synthetic) to a module according to explicit mappings, defaults, layout rules (MONOLITH or other),
     * and declared module runtimes. Collects client-name overrides, service-package→module mappings, used modules, and any validation
     * errors or warnings produced during resolution. If the configured mapping or the provided models list is null, an empty resolution
     * is returned.</p>
     *
     * @param models the list of PipelineStepModel instances to resolve module placements for
     * @return a PipelineRuntimeMappingResolution containing:
     *         - per-step module assignments,
     *         - client-name → module overrides,
     *         - service-package → module mappings,
     *         - ordered lists of validation errors and warnings,
     *         - the set of modules used by the resolved assignments
     */
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

    /**
     * Finalizes the mapping resolution by emitting diagnostics (if a processing environment is present)
     * and returning an immutable PipelineRuntimeMappingResolution with the provided data.
     *
     * @param moduleAssignments      mapping of pipeline steps to resolved module names
     * @param clientOverrides        mapping of client names to their override module names
     * @param moduleByServicePackage mapping of service-package identifiers to module names
     * @param errors                 collected error messages
     * @param warnings               collected warning messages
     * @param usedModules            set of module names that were referenced or assigned
     * @return an immutable PipelineRuntimeMappingResolution containing module assignments, client overrides,
     *         service-package-to-module mappings, and the collected errors, warnings, and used modules
     * @throws IllegalStateException if validation mode is STRICT and there is at least one recorded error
     */
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
            moduleAssignments,
            clientOverrides,
            moduleByServicePackage,
            errors,
            warnings,
            usedModules
        );
    }

    /**
     * Selects the module to assign a pipeline step, using an explicit step mapping if present
     * or falling back to the configured defaults ("shared", "per-step", or a fixed module).
     *
     * The chosen module is guaranteed to have an entry in {@code moduleRuntimes} (the method
     * will add a default runtime entry when necessary). Any resolution problems are appended
     * to the provided {@code errors} list.
     *
     * @param model the pipeline step model to resolve a module for
     * @param normalizedStepMappings mapping of normalized step keys to module names
     * @param moduleRuntimes map of module names to runtime identifiers; may be mutated to ensure entries for chosen modules
     * @param errors collector for human-readable error messages produced during resolution
     * @return the resolved module name for the step, or {@code null} if no valid module could be determined
     */
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

    /**
     * Resolve the target module name for a synthetic (side-effect) pipeline step.
     *
     * Uses an explicit synthetic mapping when available; otherwise falls back to the configured
     * synthetic default strategy: "per-step" (prefer the step's service-package module, then plugin default),
     * "plugin" (plugin default), or a fixed module name. Records validation errors when an explicit
     * mapping references an unknown module and adds warnings when falling back from "per-step" to the plugin default.
     *
     * @param model the synthetic step model to resolve
     * @param syntheticIndex index used to compute the synthetic key (payload/index/ambiguity)
     * @param normalizedSyntheticMappings explicit synthetic key -> module mappings (lower-cased/normalized)
     * @param moduleByServicePackage map of service-package -> module determined for regular steps
     * @param moduleRuntimes map of module -> runtime name; used to validate or ensure module runtime entries
     * @param errors mutable list to append resolution or validation error messages
     * @param warnings mutable list to append non-fatal warnings (e.g., fallback from per-step to plugin default)
     * @return the resolved module name for the synthetic step
     */
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

    /**
     * Validates that each module's declared runtime name exists in the provided runtimes map.
     *
     * Appends a descriptive error message to `errors` for every module that references a runtime
     * name not present in `runtimes`. No action is taken if `runtimes` is null or empty.
     *
     * @param modules a map from module name to declared runtime name
     * @param runtimes a map whose keys are valid runtime names
     * @param errors a mutable list to which error messages will be appended for unknown runtimes
     */
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

    /**
     * Checks that all non-side-effect pipeline steps resolve to a single runtime and records a diagnostic if multiple runtimes are used.
     *
     * Iterates non-side-effect entries in {@code moduleAssignments}, collects the runtime names from {@code moduleRuntimes}, and if more than one distinct runtime is found appends a message to {@code errors} when strict validation is configured or to {@code warnings} otherwise.
     *
     * @param moduleAssignments mapping of pipeline step models to resolved module names
     * @param moduleRuntimes mapping of module names to runtime names
     * @param errors list to which an error message will be added when strict validation detects multiple runtimes
     * @param warnings list to which a warning message will be added when non-strict validation detects multiple runtimes
     */
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

    /**
     * Ensures every explicit step and synthetic mapping targets the given monolith module and records the first violation.
     *
     * @param stepMappings       map of explicit step keys to module names; may be null
     * @param syntheticMappings  map of explicit synthetic keys to module names; may be null
     * @param monolithModule     expected module name for a monolith layout
     * @param errors             mutable list that will receive an error message if a mapping targets a different module
     */
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

    /**
     * Selects the module name to use when the pipeline layout is MONOLITH.
     *
     * Chooses an explicit default module from `defaults` when that value is set and not
     * `per-step` or `shared`; otherwise picks the first declared module from `moduleRuntimes`;
     * if no modules are declared, falls back to the constant "monolith". Ensures the chosen
     * module has an associated runtime entry in `moduleRuntimes`.
     *
     * @param moduleRuntimes map of module name -> runtime name; used to pick a declared module
     *                       and to record the chosen module's runtime if missing
     * @param defaults       pipeline-level defaults containing a possible explicit module name
     * @param errors         list for recording validation errors (unused by this implementation)
     * @return the resolved monolith module name
     */
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

    /**
     * Selects the single declared module to use when defaults.module is set to "shared".
     *
     * If exactly one module is declared in moduleRuntimes that module name is returned;
     * otherwise an error message is appended to errors and null is returned.
     *
     * @param moduleRuntimes mapping of declared module name to its runtime identifier
     * @param errors        list to which a diagnostic message will be added when zero or multiple modules are declared
     * @return the sole declared module name if exactly one exists, or null if none or more than one are declared
     */
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

    /**
     * Selects an explicit module mapping for a synthetic (side-effect) step using the synthetic key and normalized mappings.
     *
     * Checks, in order, a payload+index mapping, an alternate payload+index mapping, a payload mapping, an alternate payload mapping,
     * a generic mapping, and an alternate generic mapping; if a payload mapping is used but the key is ambiguous while validation is STRICT,
     * an error message is appended to `errors`.
     *
     * @param key the synthetic key describing payload/generic identifiers and ambiguity/index information; may be null
     * @param normalizedSyntheticMappings normalized mapping keys to module names for synthetics
     * @param errors a mutable list to append validation error messages to
     * @return the resolved module name for the synthetic mapping, or `null` if no explicit mapping is found
     */
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

    /**
     * Locate the first mapped module name for a pipeline step using its candidate lookup keys.
     *
     * Checks each candidate key produced for the given step (in order) against the provided
     * normalized mappings and returns the first matching module name.
     *
     * @param model the pipeline step model whose candidate keys will be checked
     * @param normalizedMappings a map of normalized lookup keys to module names
     * @return the matched module name, or `null` if no mapping is found or inputs are null/empty
     */
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

    /**
     * Generate candidate lookup keys for a pipeline step used when resolving explicit mappings.
     *
     * @param model the pipeline step model to derive candidate names from
     * @return a list of candidate keys in preferred lookup order: service name, base service name,
     *         client name, and kebab-case base name; each entry is lower-cased and trimmed and empty
     *         values are omitted
     */
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

    /**
     * Produce a normalized map with lower-cased, trimmed keys and only entries that have non-blank keys and module names.
     *
     * @param mapping the input key-to-module mapping; may be null
     * @return a new map containing entries from {@code mapping} whose keys and module values are non-blank, where keys are lower-cased and trimmed; returns an empty map if {@code mapping} is null
     */
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

    /**
     * Ensures the given module name is present in the module-to-runtime map, adding the mapping to the configured default runtime if missing.
     *
     * @param moduleRuntimes map of module names to runtime names; may be modified
     * @param moduleName the module name to ensure in the map; ignored if null or blank
     */
    private void ensureModuleRuntime(Map<String, String> moduleRuntimes, String moduleName) {
        if (moduleName == null || moduleName.isBlank()) {
            return;
        }
        moduleRuntimes.putIfAbsent(moduleName, mapping.defaults().runtime());
    }

    /**
     * Derives a default module name for a pipeline step from its service name.
     *
     * @param model  the pipeline step model whose service name is used to derive the module name
     * @param errors a mutable list to which an error message is appended if a module name cannot be derived
     * @return       the derived module name (kebab-case base service name with the `-svc` suffix), or `null` if derivation failed
     */
    private String defaultModuleForStep(PipelineStepModel model, List<String> errors) {
        String baseName = OrchestratorClientNaming.baseServiceName(model.serviceName());
        if (baseName.isBlank()) {
            errors.add("Unable to derive module name for step '" + model.serviceName() + "'");
            return null;
        }
        return OrchestratorClientNaming.toKebabCase(baseName) + "-svc";
    }

    /**
     * Derives a default module name for a side-effect step based on its aspect name.
     *
     * @param model the side-effect step model whose aspect name is used to derive the module
     * @return the default module name: "persistence-svc" for aspects starting with "persistence", "cache-invalidation-svc" for aspects starting with "cache", or "<aspectName>-svc" otherwise
     */
    private String defaultModuleForSideEffect(PipelineStepModel model) {
        String aspectName = OrchestratorClientNaming.resolveAspectName(model);
        if (aspectName.startsWith(PERSISTENCE_ASPECT_PREFIX)) {
            return PERSISTENCE_MODULE;
        }
        if (aspectName.startsWith(CACHE_ASPECT_PREFIX)) {
            return CACHE_MODULE;
        }
        return aspectName + "-svc";
    }

    /**
     * Converts the input string to lowercase and trims surrounding whitespace; returns an empty string for null input.
     *
     * @param value the string to normalize
     * @return the trimmed, lowercased string using {@link java.util.Locale#ROOT}, or an empty string if {@code value} is null
     */
    private static String safeLower(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Builds an index of synthetic side-effect steps grouped by synthetic payload identifier.
     *
     * @param syntheticSteps list of synthetic PipelineStepModel instances to index; entries without a synthetic key are ignored
     * @return a SyntheticIndex that maps payload IDs to lists of matching synthetic step models (preserving insertion order)
     */
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
        /**
         * Produces a SyntheticKey for the given synthetic step that includes its payload group index and whether that payload is ambiguous.
         *
         * @param model the pipeline step model to compute a synthetic key for
         * @return      a `SyntheticKey` whose index is the model's position within its payload group and whose ambiguity flag is true when multiple synthetics share that payload; or `null` if no synthetic key can be derived for the model
         */
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
        /**
         * Create a SyntheticKey that identifies a synthetic (side-effect) mapping for the given pipeline step.
         *
         * <p>The key encodes the synthetic's payload and generic identifiers and includes placeholders for
         * index and ambiguity; it is intended for lookup of explicit synthetic-to-module mappings.
         *
         * @param model the pipeline step model for which to build the synthetic key; may be null
         * @return the constructed SyntheticKey, or `null` if `model` is null or does not expose the aspect
         *         or payload type required to form a synthetic key
         */
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

        /**
         * Create a copy of this SyntheticKey with the given index and ambiguity flag.
         *
         * @param index     the 0-based position of this synthetic within its payload group
         * @param ambiguous true if this synthetic key refers to an ambiguous (non-unique) payload group
         * @return a new SyntheticKey identical to this one except for the provided index and ambiguity
         */
        SyntheticKey withIndex(int index, boolean ambiguous) {
            return new SyntheticKey(payloadId, genericId, altPayloadId, altGenericId, index, ambiguous);
        }

        /**
         * Derives the aspect identifier from a step's service name.
         *
         * <p>If the model has no service name, an empty string is returned. The method strips a
         * trailing "SideEffectService" suffix and then strips the side-effect type name (if present)
         * from the end of the service name to produce the aspect id.
         *
         * @return the derived aspect identifier, or an empty string if the model has no service name
         */
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
