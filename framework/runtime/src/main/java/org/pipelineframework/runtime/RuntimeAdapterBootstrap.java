/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.runtime;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.AmbiguousResolutionException;
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.runtime.core.EventBusBridge;
import org.pipelineframework.runtime.core.ExecutionContextCarrier;
import org.pipelineframework.runtime.core.ReactiveRuntime;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.pipelineframework.runtime.core.SchedulerBoundary;
import org.pipelineframework.runtime.core.TransactionBoundary;
import org.pipelineframework.runtime.core.WorkDispatcher;

/**
 * Bootstrapper that wires Quarkus runtime implementations into framework-core adapters.
 */
@ApplicationScoped
public class RuntimeAdapterBootstrap {
    private static final Logger LOG = Logger.getLogger(RuntimeAdapterBootstrap.class);
    private static final String EXECUTOR_THREAD_NAME = "quarkus-virtual-thread-runtime";

    @Inject
    PipelineConfig pipelineConfig;
    @Inject
    PipelineStepConfig pipelineStepConfig;

    private final Executor virtualThreadExecutor = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name(EXECUTOR_THREAD_NAME, 0).factory()
    );
    private final WorkDispatcher workDispatcher = work -> CompletableFuture.completedFuture(null);

    public RuntimeAdapterBootstrap() {
    }

    void onStart(@Observes StartupEvent event) {
        RuntimeAdapters.registerBeanLookup(new QuarkusBeanLookup());
        RuntimeAdapters.registerConfigProvider(new QuarkusConfigProvider());
        RuntimeAdapters.registerExecutionContextCarrier(new QuarkusExecutionContextCarrier());
        RuntimeAdapters.registerReactiveRuntime(new QuarkusReactiveRuntime());
        RuntimeAdapters.registerSchedulerBoundary(new QuarkusSchedulerBoundary());
        RuntimeAdapters.registerTransactionBoundary(new QuarkusTransactionBoundary());
        RuntimeAdapters.registerEventBusBridge(new QuarkusEventBusBridge());
        RuntimeAdapters.registerWorkDispatcher(new QuarkusWorkDispatcher());

        // Ensure baseline values are available during startup.
        if (pipelineConfig == null) {
            LOG.warn("PipelineConfig was not injected; adapter registry still remains active.");
        }
        if (pipelineStepConfig == null) {
            LOG.warn("PipelineStepConfig was not injected; adapter registry still remains active.");
        }
    }

    @PreDestroy
    void onStop() {
        try {
            if (virtualThreadExecutor instanceof AutoCloseable closeable) {
                closeable.close();
            }
        } catch (Exception ignored) {
            LOG.debug("Failed to close virtual-thread executor cleanly.");
        } finally {
            RuntimeAdapters.resetForTests();
        }
    }

    private final class QuarkusBeanLookup implements org.pipelineframework.runtime.core.BeanLookup {
        @Override
        public <T> Optional<T> get(Class<T> beanType) {
            try {
                Instance<T> instance = CDI.current().select(beanType);
                if (instance == null || instance.isUnsatisfied() || instance.isAmbiguous()) {
                    if (instance != null && instance.isAmbiguous()) {
                        LOG.warnf("Ambiguous bean resolution for %s during adapter bootstrap.", beanType.getName());
                    }
                    return Optional.empty();
                }
                return Optional.ofNullable(instance.get());
            } catch (AmbiguousResolutionException | ContextNotActiveException | IllegalStateException ex) {
                LOG.debugf("Skipping bean lookup for %s: %s", beanType.getName(), ex.toString());
                return Optional.empty();
            }
        }
    }

    private final class QuarkusConfigProvider implements org.pipelineframework.runtime.core.ConfigProvider {
        @Override
        public <T> Optional<T> get(Class<T> configType) {
            return new QuarkusBeanLookup().get(configType);
        }
    }

    private final class QuarkusExecutionContextCarrier implements ExecutionContextCarrier {
        private static final ThreadLocal<java.util.Map<String, Object>> FALLBACK =
            ThreadLocal.withInitial(java.util.HashMap::new);
        private final Set<String> trackedKeys = ConcurrentHashMap.newKeySet();

        @Override
        public <T> T get(String key, Class<T> type) {
            Context context = Vertx.currentContext();
            if (context != null) {
                try {
                    Object stored = context.getLocal(key);
                    if (stored != null && type != null && type.isInstance(stored)) {
                        return type.cast(stored);
                    }
                } catch (UnsupportedOperationException ignored) {
                    // Fall back to local cache below.
                }
            }
            Object value = FALLBACK.get().get(key);
            if (value == null || type == null || !type.isInstance(value)) {
                return null;
            }
            return type.cast(value);
        }

        @Override
        public void put(String key, Object value) {
            if (key == null) {
                return;
            }
            Context context = Vertx.currentContext();
            if (context != null) {
                try {
                    if (value == null) {
                        context.removeLocal(key);
                        trackedKeys.remove(key);
                    } else {
                        context.putLocal(key, value);
                        trackedKeys.add(key);
                    }
                    return;
                } catch (UnsupportedOperationException ignored) {
                    // Fall back to thread-local cache below.
                }
            }
            if (value == null) {
                FALLBACK.get().remove(key);
                trackedKeys.remove(key);
            } else {
                FALLBACK.get().put(key, value);
                trackedKeys.add(key);
            }
        }

        @Override
        public void clear(String key) {
            if (key == null) {
                return;
            }
            Context context = Vertx.currentContext();
            if (context != null) {
                try {
                    context.removeLocal(key);
                    trackedKeys.remove(key);
                    return;
                } catch (UnsupportedOperationException ignored) {
                    // Fall back to thread-local cache.
                }
            }
            FALLBACK.get().remove(key);
            trackedKeys.remove(key);
        }

        @Override
        public void clear() {
            Context context = Vertx.currentContext();
            if (context != null) {
                try {
                    for (String key : trackedKeys) {
                        context.removeLocal(key);
                    }
                } catch (UnsupportedOperationException ignored) {
                    // Fall back to thread-local cache cleanup below.
                }
            }
            FALLBACK.remove();
            trackedKeys.clear();
        }
    }

    private final class QuarkusReactiveRuntime implements ReactiveRuntime {
        @Override
        public <T> CompletionStage<T> executeBlocking(java.util.function.Supplier<T> supplier, boolean offloadToVirtualThread) {
            if (offloadToVirtualThread) {
                return Uni.createFrom()
                    .item(() -> {
                        try {
                            return supplier.get();
                        } catch (RuntimeException ex) {
                            throw ex;
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    })
                    .runSubscriptionOn(virtualThreadExecutor)
                    .subscribeAsCompletionStage();
            }
            return Uni.createFrom()
                .item(() -> {
                    try {
                        return supplier.get();
                    } catch (RuntimeException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .subscribeAsCompletionStage();
        }
    }

    private final class QuarkusSchedulerBoundary implements SchedulerBoundary {
        @Override
        public <T> CompletionStage<T> schedule(Callable<T> task) {
            try {
                Context context = Vertx.currentContext();
                if (context == null) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return task.call();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                CompletableFuture<T> promise = new CompletableFuture<>();
                context.runOnContext(v -> {
                    try {
                        promise.complete(task.call());
                    } catch (Exception e) {
                        promise.completeExceptionally(e);
                    }
                });
                return promise;
            } catch (Exception e) {
                CompletableFuture<T> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return failed;
            }
        }
    }

    private final class QuarkusTransactionBoundary implements TransactionBoundary {
        @Override
        public <T> T executeInTransaction(Callable<T> callback) {
            try {
                return callback.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final class QuarkusEventBusBridge implements EventBusBridge {
        @Override
        public void publish(String address, Object event) {
            // Intentionally no-op for now; event bus consumers are still adapter-specific.
        }
    }

    private final class QuarkusWorkDispatcher implements WorkDispatcher {
        @Override
        public CompletionStage<Void> enqueue(Object item) {
            return workDispatcher.enqueue(item);
        }
    }
}
