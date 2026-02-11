package org.pipelineframework.processor.phase;

import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;

/**
 * Centralized policy rules for generation decisions.
 */
public class GenerationPolicy {

    /**
     * Determines whether plugin-server artifacts should be generated.
     *
     * @param ctx compilation context
     * @return true when plugin-server artifacts are allowed
     */
    public boolean allowPluginServerArtifacts(PipelineCompilationContext ctx) {
        if (ctx == null) {
            return false;
        }
        if (ctx.isPluginHost()) {
            return true;
        }
        String moduleName = ctx.getModuleName();
        return ctx.getRuntimeMapping() != null && moduleName != null && !moduleName.isBlank();
    }

    /**
     * Resolves the client role corresponding to a server role.
     *
     * @param serverRole source role
     * @return mapped client role
     */
    public DeploymentRole resolveClientRole(DeploymentRole serverRole) {
        if (serverRole == null) {
            return null;
        }
        return switch (serverRole) {
            case PLUGIN_SERVER -> DeploymentRole.PLUGIN_CLIENT;
            case PIPELINE_SERVER -> DeploymentRole.ORCHESTRATOR_CLIENT;
            case ORCHESTRATOR_CLIENT -> DeploymentRole.ORCHESTRATOR_CLIENT;
            case PLUGIN_CLIENT -> DeploymentRole.PLUGIN_CLIENT;
            case REST_SERVER -> DeploymentRole.ORCHESTRATOR_CLIENT;
        };
    }
}
