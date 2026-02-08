package org.pipelineframework.csv.pipelineruntime.config;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.configuration.ConfigUtils;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

/**
 * Validates TLS/security runtime configuration for pipeline-runtime-svc at startup.
 */
@ApplicationScoped
public class PipelineRuntimeSecurityConfigValidator {

    private static final Logger LOG = Logger.getLogger(PipelineRuntimeSecurityConfigValidator.class);

    private static final String KEYSTORE_PATH_KEY = "quarkus.http.ssl.certificate.key-store-file";
    private static final String KEYSTORE_PASSWORD_KEY = "quarkus.http.ssl.certificate.key-store-password";
    private static final String TRUSTSTORE_PATH_KEY = "quarkus.tls.pipeline-client.trust-store.jks.path";
    private static final String TRUSTSTORE_PASSWORD_KEY = "quarkus.tls.pipeline-client.trust-store.jks.password";

    @Inject
    Config config;

    void onStart(@Observes StartupEvent ignored) {
        String keyStorePath = required(KEYSTORE_PATH_KEY, "SERVER_KEYSTORE_PATH");
        required(KEYSTORE_PASSWORD_KEY, "SERVER_KEYSTORE_PASSWORD");
        String trustStorePath = required(TRUSTSTORE_PATH_KEY, "CLIENT_TRUSTSTORE_PATH");
        required(TRUSTSTORE_PASSWORD_KEY, "CLIENT_TRUSTSTORE_PASSWORD");

        List<String> activeProfiles = ConfigUtils.getProfiles();
        boolean devOrTestActive = activeProfiles.contains("dev") || activeProfiles.contains("test");
        if (!devOrTestActive) {
            requireAbsolutePath(KEYSTORE_PATH_KEY, keyStorePath);
            requireAbsolutePath(TRUSTSTORE_PATH_KEY, trustStorePath);
        }
    }

    private String required(String key, String envName) {
        String value = config.getOptionalValue(key, String.class).orElse("");
        if (value.isBlank()) {
            throw new IllegalStateException("Missing required TLS setting '" + key
                + "'. Provide environment variable " + envName + ".");
        }
        return value.trim();
    }

    private void requireAbsolutePath(String key, String value) {
        Path path = Paths.get(value);
        Path normalized = path.normalize();
        if (!normalized.isAbsolute()) {
            throw new IllegalStateException("TLS path '" + key + "' must be absolute in non-dev profiles: " + value);
        }
        if (containsParentTraversal(path) || containsParentTraversal(normalized) || normalized.toString().contains("..")) {
            throw new IllegalStateException("TLS path '" + key + "' must not contain parent traversal segments: " + value);
        }
        LOG.debugf("Validated absolute TLS path for %s", key);
    }

    private boolean containsParentTraversal(Path path) {
        for (Path segment : path) {
            if ("..".equals(segment.toString())) {
                return true;
            }
        }
        return false;
    }
}
