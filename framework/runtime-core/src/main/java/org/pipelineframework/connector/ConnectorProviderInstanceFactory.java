package org.pipelineframework.connector;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Host-internal creation seam for binding-owned provider instances. */
interface ConnectorProviderInstanceFactory {
    ConnectorProviderLease create(ConnectorProvider<?> prototype);

    static ConnectorProviderInstanceFactory plainJava() {
        return prototype -> {
            Class<?> type = Objects.requireNonNull(prototype, "connector provider prototype must not be null").getClass();
            try {
                Object instance = type.getConstructor().newInstance();
                if (!(instance instanceof ConnectorProvider<?> provider)) {
                    throw new IllegalStateException("constructed type is not a connector provider: " + type.getName());
                }
                return ConnectorProviderLease.of(provider);
            } catch (NoSuchMethodException failure) {
                throw new IllegalArgumentException(
                    "connector provider " + type.getName()
                        + " needs a public no-argument constructor for named bindings",
                    failure);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException failure) {
                throw new IllegalArgumentException(
                    "failed to create binding-owned connector provider " + type.getName(), failure);
            }
        };
    }
}

/** One host-owned provider instance and its container cleanup action. */
final class ConnectorProviderLease {
    private final ConnectorProvider<?> provider;
    private final Runnable release;
    private final AtomicBoolean released = new AtomicBoolean();

    private ConnectorProviderLease(ConnectorProvider<?> provider, Runnable release) {
        this.provider = Objects.requireNonNull(provider, "connector provider must not be null");
        this.release = Objects.requireNonNull(release, "connector provider release action must not be null");
    }

    static ConnectorProviderLease of(ConnectorProvider<?> provider) {
        return new ConnectorProviderLease(provider, () -> { });
    }

    static ConnectorProviderLease of(ConnectorProvider<?> provider, Runnable release) {
        return new ConnectorProviderLease(provider, release);
    }

    ConnectorProvider<?> provider() {
        return provider;
    }

    void release() {
        if (released.compareAndSet(false, true)) {
            release.run();
        }
    }
}
