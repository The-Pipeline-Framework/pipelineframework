package org.pipelineframework.connector;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

/** Installs an isolated pipeline configuration before the HTTP test application starts. */
public final class OwnedPayloadHttpTestResource implements QuarkusTestResourceLifecycleManager {
    private String previousPipeline;
    private String previousKey;

    @Override
    public Map<String, String> start() {
        previousPipeline = System.getProperty("pipeline.config");
        previousKey = System.getProperty("tpf.object.reference.hmac-key");
        try {
            Path pipeline = Path.of(getClass().getResource("/owned-payload-http-pipeline.yaml").toURI());
            System.setProperty("pipeline.config", pipeline.toString());
        } catch (URISyntaxException failure) {
            throw new IllegalStateException("owned payload HTTP fixture is unavailable", failure);
        }
        System.setProperty("tpf.object.reference.hmac-key",
            Base64.getEncoder().encodeToString(new byte[32]));
        return Map.of();
    }

    @Override
    public void stop() {
        restore("pipeline.config", previousPipeline);
        restore("tpf.object.reference.hmac-key", previousKey);
    }

    private static void restore(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }
}
