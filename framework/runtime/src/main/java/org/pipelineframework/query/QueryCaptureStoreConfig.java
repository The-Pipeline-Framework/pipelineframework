package org.pipelineframework.query;

import java.time.Duration;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Selects and configures the framework-owned Query capture store. */
@ConfigMapping(prefix = "pipeline.query.capture-store")
public interface QueryCaptureStoreConfig {
    /** Built-in store selection; custom requires exactly one application-supplied store. */
    @WithDefault("memory")
    Provider provider();

    /** DynamoDB-specific settings used by the durable store. */
    Dynamo dynamo();

    enum Provider {
        MEMORY,
        DYNAMO,
        CUSTOM
    }

    interface Dynamo {
        /** Table containing immutable Query capture revisions. */
        @WithDefault("tpf_query_capture")
        String table();

        /** Time for which an active streaming writer retains distributed ownership. */
        @WithDefault("5m")
        Duration streamingLeaseDuration();

        /** Delay between strongly consistent checks made by streaming capture waiters. */
        @WithDefault("250ms")
        Duration streamingPollInterval();
    }
}
