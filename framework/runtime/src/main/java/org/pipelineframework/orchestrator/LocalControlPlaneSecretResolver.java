package org.pipelineframework.orchestrator;

import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Local secret resolver for runtime configuration references.
 */
@ApplicationScoped
public class LocalControlPlaneSecretResolver implements ControlPlaneSecretResolver {

    private static final String ENV_PREFIX = "env:";
    private static final String SYS_PREFIX = "sys:";
    private static final String CONFIG_PREFIX = "config:";

    @Override
    public String resolve(String reference) {
        if (reference == null || reference.isBlank()) {
            throw new IllegalArgumentException("Secret reference must not be blank");
        }
        String trimmed = reference.trim();
        String value;
        if (trimmed.startsWith(ENV_PREFIX)) {
            value = resolveEnv(trimmed.substring(ENV_PREFIX.length()));
        } else if (trimmed.startsWith(SYS_PREFIX)) {
            value = resolveSystemProperty(trimmed.substring(SYS_PREFIX.length()));
        } else if (trimmed.startsWith(CONFIG_PREFIX)) {
            value = resolveConfig(trimmed.substring(CONFIG_PREFIX.length()));
        } else {
            throw new IllegalArgumentException("Unsupported secret reference scheme: " + safeScheme(trimmed));
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Secret reference could not be resolved: " + trimmed);
        }
        return value;
    }

    private String resolveEnv(String name) {
        String key = requireKey(name, ENV_PREFIX);
        return System.getenv(key);
    }

    private String resolveSystemProperty(String name) {
        String key = requireKey(name, SYS_PREFIX);
        return System.getProperty(key);
    }

    private String resolveConfig(String name) {
        String key = requireKey(name, CONFIG_PREFIX);
        Optional<String> configured = ConfigProvider.getConfig().getOptionalValue(key, String.class);
        return configured.orElse(null);
    }

    private static String requireKey(String name, String prefix) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Secret reference " + prefix + " requires a key");
        }
        return name.trim();
    }

    private static String safeScheme(String reference) {
        int delimiter = reference.indexOf(':');
        return delimiter < 0 ? "<missing>" : reference.substring(0, delimiter + 1);
    }
}
