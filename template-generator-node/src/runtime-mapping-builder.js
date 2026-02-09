/*
 * Shared runtime-mapping builder used by both template engines.
 */

function buildModularModules(steps, includePersistenceModule, includeCacheInvalidationModule, stepId) {
    const modules = {};
    for (const step of steps) {
        const moduleName = step.serviceName;
        const resolvedStepId = stepId(step);
        if (!moduleName || !resolvedStepId) {
            continue;
        }
        modules[moduleName] = {
            runtime: moduleName,
            steps: [resolvedStepId]
        };
    }
    modules['orchestrator-svc'] = { runtime: 'orchestrator-svc' };
    if (includePersistenceModule) {
        modules['persistence-svc'] = {
            runtime: 'persistence-svc',
            aspects: ['persistence']
        };
    }
    if (includeCacheInvalidationModule) {
        modules['cache-invalidation-svc'] = {
            runtime: 'cache-invalidation-svc',
            aspects: ['cache', 'cache-invalidate', 'cache-invalidate-all']
        };
    }
    return modules;
}

function buildPipelineModules(steps, includePersistenceModule, includeCacheInvalidationModule, stepId) {
    const modules = {
        'pipeline-runtime-svc': {
            runtime: 'pipeline-runtime-svc',
            steps: []
        },
        'orchestrator-svc': {
            runtime: 'orchestrator-svc'
        }
    };
    for (const step of steps) {
        const resolvedStepId = stepId(step);
        if (!resolvedStepId) {
            continue;
        }
        modules['pipeline-runtime-svc'].steps.push(resolvedStepId);
    }
    if (includePersistenceModule) {
        modules['persistence-svc'] = {
            runtime: 'persistence-svc',
            aspects: ['persistence']
        };
    }
    if (includeCacheInvalidationModule) {
        modules['cache-invalidation-svc'] = {
            runtime: 'cache-invalidation-svc',
            aspects: ['cache', 'cache-invalidate', 'cache-invalidate-all']
        };
    }
    return modules;
}

function buildMonolithModules(steps, includePersistenceModule, includeCacheInvalidationModule, stepId) {
    const modules = {
        'monolith-svc': {
            runtime: 'monolith-svc',
            steps: []
        },
        'orchestrator-svc': {
            runtime: 'monolith-svc'
        }
    };
    for (const step of steps) {
        const resolvedStepId = stepId(step);
        if (!resolvedStepId) {
            continue;
        }
        modules['monolith-svc'].steps.push(resolvedStepId);
    }
    const aspects = [];
    if (includePersistenceModule) {
        aspects.push('persistence');
    }
    if (includeCacheInvalidationModule) {
        aspects.push('cache', 'cache-invalidate', 'cache-invalidate-all');
    }
    if (aspects.length > 0) {
        modules['monolith-svc'].aspects = aspects;
    }
    return modules;
}

function buildRuntimeMappingCore(options) {
    const {
        layout,
        steps,
        includePersistenceModule,
        includeCacheInvalidationModule,
        isRuntimeLayout,
        normalizeRuntimeLayout,
        stepId
    } = options;

    if (!Array.isArray(steps)) {
        throw new Error('buildRuntimeMapping requires steps to be an array.');
    }
    if (!isRuntimeLayout(layout)) {
        throw new Error(`Unsupported runtime layout '${layout}'.`);
    }

    const runtimeLayout = normalizeRuntimeLayout(layout);

    switch (runtimeLayout) {
        case 'modular':
            return {
                layout: 'modular',
                validation: 'auto',
                defaults: {
                    runtime: 'orchestrator-svc',
                    module: 'orchestrator-svc',
                    synthetic: {
                        module: includePersistenceModule ? 'persistence-svc' : 'orchestrator-svc'
                    }
                },
                modules: buildModularModules(
                    steps,
                    includePersistenceModule,
                    includeCacheInvalidationModule,
                    stepId
                )
            };
        case 'pipeline-runtime':
            return {
                layout: 'pipeline-runtime',
                validation: 'auto',
                defaults: {
                    runtime: 'pipeline-runtime-svc',
                    module: 'pipeline-runtime-svc',
                    synthetic: {
                        // In pipeline-runtime layout, synthetic/plugin concerns prefer their dedicated plugin host runtime.
                        module: includePersistenceModule
                            ? 'persistence-svc'
                            : includeCacheInvalidationModule
                                ? 'cache-invalidation-svc'
                                : 'pipeline-runtime-svc'
                    }
                },
                modules: buildPipelineModules(
                    steps,
                    includePersistenceModule,
                    includeCacheInvalidationModule,
                    stepId
                )
            };
        case 'monolith':
            return {
                layout: 'monolith',
                validation: 'auto',
                defaults: {
                    runtime: 'monolith-svc',
                    module: 'monolith-svc',
                    synthetic: {
                        module: 'monolith-svc'
                    }
                },
                modules: buildMonolithModules(
                    steps,
                    includePersistenceModule,
                    includeCacheInvalidationModule,
                    stepId
                )
            };
        default:
            throw new Error(`Unsupported runtime layout '${layout}'.`);
    }
}

module.exports = {
    buildRuntimeMappingCore
};
