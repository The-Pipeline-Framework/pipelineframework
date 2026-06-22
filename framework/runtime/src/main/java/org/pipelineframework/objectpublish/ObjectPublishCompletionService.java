package org.pipelineframework.objectpublish;

import java.util.List;
import java.util.Objects;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;

/**
 * Publishes queue-async terminal output objects before an execution is marked successful.
 */
@ApplicationScoped
public class ObjectPublishCompletionService {
    @Inject
    Instance<ObjectPublishTelemetry> telemetry;

    private volatile ObjectPublishRunner runner;

    public Uni<Void> publishIfConfigured(List<?> outputItems) {
        ObjectPublishRunner active = runner();
        if (!active.enabled()) {
            return Uni.createFrom().voidItem();
        }
        return active.publishItems(outputItems == null ? List.of() : outputItems);
    }

    private ObjectPublishRunner runner() {
        ObjectPublishRunner active = runner;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            active = runner;
            if (active == null) {
                active = Objects.requireNonNull(
                    ObjectPublishRunner.loadFromDefaultConfig(
                        telemetry != null && telemetry.isResolvable() ? telemetry.get() : ObjectPublishTelemetry.NOOP),
                    "ObjectPublishRunner.loadFromDefaultConfig() must not return null");
                runner = active;
            }
            return active;
        }
    }
}
