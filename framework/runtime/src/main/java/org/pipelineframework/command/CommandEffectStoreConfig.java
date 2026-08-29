package org.pipelineframework.command;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Selects and configures the framework-owned Command effect store.
 */
@ConfigMapping(prefix = "pipeline.command.effect-store")
public interface CommandEffectStoreConfig {

    /**
     * Built-in store selection. {@link Provider#CUSTOM} disables both built-in beans so
     * an application or extension can provide exactly one {@link CommandEffectStore}.
     */
    @WithDefault("memory")
    Provider provider();

    /** DynamoDB-specific settings used when {@link #provider()} is {@link Provider#DYNAMO}. */
    Dynamo dynamo();

    enum Provider {
        MEMORY,
        DYNAMO,
        CUSTOM
    }

    interface Dynamo {
        /** DynamoDB table containing immutable Command effect revisions. */
        @WithDefault("tpf_command_effect")
        String table();
    }
}
