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
     *
     * <p>Required fields vary by transport kind:
     * {@code HTTP} targets require {@code base-url}; {@code path}, {@code method},
     * {@code content-type}, and {@code idempotency-header} are optional.
     * {@code GRPC} targets require {@code host} and {@code port}; {@code plaintext} is optional.
     */
    interface TargetConfig {

        /**
         * Target transport kind (required).
         *
         * @return transport kind
         */
        PublicationTargetKind kind();

        /**
         * Optional explicit encoding override for {@link PublicationTargetKind#HTTP} targets.
         *
         * <p>Ignored for {@link PublicationTargetKind#GRPC} targets, which always use protobuf encoding.
         *
         * @return encoding when configured
         */
        Optional<PublicationEncoding> encoding();

        /**
         * Optional content type override for {@link PublicationTargetKind#HTTP} targets.
         *
         * @return content type when configured
         */
        @WithName("content-type")
        Optional<String> contentType();

        /**
         * Optional idempotency header override for {@link PublicationTargetKind#HTTP} targets.
         *
         * @return idempotency header name when configured
         */
        @WithName("idempotency-header")
        Optional<String> idempotencyHeader();

        /**
         * gRPC host for {@link PublicationTargetKind#GRPC} targets (required).
         *
         * @return host when configured
         */
        Optional<String> host();

        /**
         * gRPC port for {@link PublicationTargetKind#GRPC} targets (required).
         *
         * @return port when configured
         */
        Optional<Integer> port();

        /**
         * Whether plaintext gRPC should be used for {@link PublicationTargetKind#GRPC} targets.
         *
         * @return true when plaintext is enabled
         */
        @WithDefault("false")
        boolean plaintext();

        /**
         * HTTP base URL for {@link PublicationTargetKind#HTTP} targets (required).
         *
         * @return base URL when configured
         */
        @WithName("base-url")
        Optional<String> baseUrl();

        /**
         * HTTP path override for {@link PublicationTargetKind#HTTP} targets.
         *
         * @return path when configured
         */
        Optional<String> path();

        /**
         * HTTP method override for {@link PublicationTargetKind#HTTP} targets.
         *
         * @return HTTP method
         */
        @WithDefault("POST")
        String method();
    }
}
