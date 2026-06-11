package org.pipelineframework.orchestrator;

import java.util.Optional;

/**
 * Shared validation and lookup for transition-worker signing secrets.
 */
public final class WorkerSecretSupport {

    private WorkerSecretSupport() {
    }

    public static Optional<String> validationError(
        String literalSecret,
        String secretRef,
        String literalConfigKey,
        String refConfigKey,
        String enabledDescription
    ) {
        boolean literalConfigured = configured(literalSecret);
        boolean refConfigured = configured(secretRef);
        if (literalConfigured && refConfigured) {
            return Optional.of("Configure only one of " + literalConfigKey + " or " + refConfigKey
                + " when " + enabledDescription);
        }
        if (!literalConfigured && !refConfigured) {
            return Optional.of(literalConfigKey + " or " + refConfigKey
                + " is required when " + enabledDescription);
        }
        return Optional.empty();
    }

    public static String resolve(
        String literalSecret,
        String secretRef,
        ControlPlaneSecretResolver resolver,
        String literalConfigKey,
        String refConfigKey
    ) {
        Optional<String> validationError = validationError(
            literalSecret,
            secretRef,
            literalConfigKey,
            refConfigKey,
            "transition worker secret resolution");
        if (validationError.isPresent()) {
            throw new IllegalStateException(validationError.get());
        }
        if (configured(literalSecret)) {
            return literalSecret;
        }
        ControlPlaneSecretResolver activeResolver = resolver == null
            ? new LocalControlPlaneSecretResolver()
            : resolver;
        return activeResolver.resolve(secretRef);
    }

    public static Optional<String> validationError(
        Optional<String> literalSecret,
        Optional<String> secretRef,
        String literalConfigKey,
        String refConfigKey,
        String enabledDescription
    ) {
        return validationError(
            literalSecret.orElse(null),
            secretRef.orElse(null),
            literalConfigKey,
            refConfigKey,
            enabledDescription);
    }

    public static String resolve(
        Optional<String> literalSecret,
        Optional<String> secretRef,
        ControlPlaneSecretResolver resolver,
        String literalConfigKey,
        String refConfigKey
    ) {
        return resolve(
            literalSecret.orElse(null),
            secretRef.orElse(null),
            resolver,
            literalConfigKey,
            refConfigKey);
    }

    private static boolean configured(String value) {
        return value != null && !value.isBlank();
    }
}
