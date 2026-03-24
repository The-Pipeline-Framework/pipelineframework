package org.pipelineframework.checkpoint;

import java.util.Map;
import java.util.Optional;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * Runtime-configured bindings from logical checkpoint publications to concrete downstream targets.
 */
@ConfigMapping(prefix = "pipeline.handoff")
@Unremovable
public interface PipelineHandoffConfig {

    /**
     * Publication-to-target binding map.
     *
     * @return configured bindings keyed by logical publication name
     */
    @WithName("bindings")
    Map<String, PublicationBinding> bindings();

    /**
     * One logical publication binding.
     */
    interface PublicationBinding {

        /**
         * Configured concrete targets for the publication.
         *
         * @return targets keyed by target id
         */
        Map<String, TargetConfig> targets();
    }

    /**
     * One concrete publication target.
     */
    interface TargetConfig {

        /**
         * Target transport kind.
         *
         * @return transport kind
         */
        PublicationTargetKind kind();

        /**
         * Optional explicit encoding override.
         *
         * @return encoding when configured
         */
        Optional<PublicationEncoding> encoding();

        /**
         * Optional content type override.
         *
         * @return content type when configured
         */
        @WithName("content-type")
        Optional<String> contentType();

        /**
         * Optional idempotency header override.
         *
         * @return idempotency header name when configured
         */
        @WithName("idempotency-header")
        Optional<String> idempotencyHeader();

        /**
         * gRPC host.
         *
         * @return host when configured
         */
        Optional<String> host();

        /**
         * gRPC port.
         *
         * @return port when configured
         */
        Optional<Integer> port();

        /**
         * Whether plaintext gRPC should be used.
         *
         * @return true when plaintext is enabled
         */
        @WithDefault("false")
        boolean plaintext();

        /**
         * HTTP base URL.
         *
         * @return base URL when configured
         */
        @WithName("base-url")
        Optional<String> baseUrl();

        /**
         * HTTP path override.
         *
         * @return path when configured
         */
        Optional<String> path();

        /**
         * HTTP method override.
         *
         * @return HTTP method
         */
        @WithDefault("POST")
        String method();
    }
}
