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

package org.pipelineframework.runtime.core;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Runtime-agnostic adapter registry.
 *
 * <p>Runtime modules (for example, Quarkus) register implementations that bridge
 * the core interfaces to their native primitives at startup. The defaults are safe
 * and intentionally conservative for non-container/unit-test execution.</p>
 */
public final class RuntimeAdapters {
    private static volatile BeanLookup beanLookup = new NoopBeanLookup();
    private static volatile ConfigProvider configProvider = new NoopConfigProvider();
    private static volatile ExecutionContextCarrier executionContextCarrier = ThreadLocalExecutionContextCarrier.INSTANCE;
    private static volatile SchedulerBoundary schedulerBoundary = new NoopSchedulerBoundary();
    private static volatile TransactionBoundary transactionBoundary = new NoopTransactionBoundary();
    private static volatile EventBusBridge eventBusBridge = new NoopEventBusBridge();
    private static volatile WorkDispatcher workDispatcher = new NoopWorkDispatcher();
    private static volatile ReactiveRuntime reactiveRuntime = new NoopReactiveRuntime();

    private RuntimeAdapters() {
    }

    public static <T> Optional<T> resolveBean(Class<T> beanType) {
        return beanLookup.get(beanType);
    }

    public static <T> Optional<T> resolveConfig(Class<T> configType) {
        return configProvider.get(configType);
    }

    public static ExecutionContextCarrier executionContextCarrier() {
        return executionContextCarrier;
    }

    public static <T> T executionContext(String key, Class<T> type) {
        return executionContextCarrier.get(key, type);
    }

    public static void setExecutionContext(String key, Object value) {
        executionContextCarrier.put(key, value);
    }

    public static void clearExecutionContext(String key) {
        executionContextCarrier.clear(key);
    }

    public static void clearExecutionContext() {
        executionContextCarrier.clear();
    }

    public static <T> java.util.concurrent.CompletionStage<T> executeBlocking(
        Callable<T> task,
        boolean offloadToVirtualThread
    ) {
        return reactiveRuntime.executeBlocking(() -> {
            try {
                return task.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, offloadToVirtualThread);
    }

    public static <T> java.util.concurrent.CompletionStage<T> schedule(Callable<T> task) {
        return schedulerBoundary.schedule(task);
    }

    public static <T> T runInTransaction(java.util.concurrent.Callable<T> task) {
        return transactionBoundary.executeInTransaction(task);
    }

    public static EventBusBridge eventBusBridge() {
        return eventBusBridge;
    }

    public static WorkDispatcher workDispatcher() {
        return workDispatcher;
    }

    public static void registerBeanLookup(BeanLookup beanLookup) {
        RuntimeAdapters.beanLookup = beanLookup == null ? new NoopBeanLookup() : beanLookup;
    }

    public static void registerConfigProvider(ConfigProvider configProvider) {
        RuntimeAdapters.configProvider = configProvider == null ? new NoopConfigProvider() : configProvider;
    }

    public static void registerExecutionContextCarrier(ExecutionContextCarrier executionContextCarrier) {
        RuntimeAdapters.executionContextCarrier =
            executionContextCarrier == null ? ThreadLocalExecutionContextCarrier.INSTANCE : executionContextCarrier;
    }

    public static void registerSchedulerBoundary(SchedulerBoundary schedulerBoundary) {
        RuntimeAdapters.schedulerBoundary = schedulerBoundary == null ? new NoopSchedulerBoundary() : schedulerBoundary;
    }

    public static void registerTransactionBoundary(TransactionBoundary transactionBoundary) {
        RuntimeAdapters.transactionBoundary = transactionBoundary == null ? new NoopTransactionBoundary() : transactionBoundary;
    }

    public static void registerReactiveRuntime(ReactiveRuntime reactiveRuntime) {
        RuntimeAdapters.reactiveRuntime = reactiveRuntime == null ? new NoopReactiveRuntime() : reactiveRuntime;
    }

    public static void registerEventBusBridge(EventBusBridge eventBusBridge) {
        RuntimeAdapters.eventBusBridge = eventBusBridge == null ? new NoopEventBusBridge() : eventBusBridge;
    }

    public static void registerWorkDispatcher(WorkDispatcher workDispatcher) {
        RuntimeAdapters.workDispatcher = workDispatcher == null ? new NoopWorkDispatcher() : workDispatcher;
    }

    public static void resetForTests() {
        registerBeanLookup(new NoopBeanLookup());
        registerConfigProvider(new NoopConfigProvider());
        registerExecutionContextCarrier(ThreadLocalExecutionContextCarrier.INSTANCE);
        registerSchedulerBoundary(new NoopSchedulerBoundary());
        registerTransactionBoundary(new NoopTransactionBoundary());
        registerEventBusBridge(new NoopEventBusBridge());
        registerWorkDispatcher(new NoopWorkDispatcher());
        registerReactiveRuntime(new NoopReactiveRuntime());
    }

    private static final class NoopBeanLookup implements BeanLookup {
        @Override
        public <T> Optional<T> get(Class<T> beanType) {
            Objects.requireNonNull(beanType, "beanType");
            return Optional.empty();
        }
    }

    private static final class NoopConfigProvider implements ConfigProvider {
        @Override
        public <T> Optional<T> get(Class<T> configType) {
            Objects.requireNonNull(configType, "configType");
            return Optional.empty();
        }
    }

    private static final class NoopSchedulerBoundary implements SchedulerBoundary {
        @Override
        public <T> java.util.concurrent.CompletionStage<T> schedule(Callable<T> task) {
            Objects.requireNonNull(task, "task");
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return task.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static final class NoopTransactionBoundary implements TransactionBoundary {
        @Override
        public <T> T executeInTransaction(Callable<T> callback) {
            try {
                return callback.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class NoopEventBusBridge implements EventBusBridge {
        @Override
        public void publish(String address, Object event) {
            // No-op bridge in core-only runtime
        }
    }

    private static final class NoopWorkDispatcher implements WorkDispatcher {
        @Override
        public CompletionStage<Void> enqueue(Object work) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class NoopReactiveRuntime implements ReactiveRuntime {
        @Override
        public <T> java.util.concurrent.CompletionStage<T> executeBlocking(java.util.function.Supplier<T> supplier, boolean offloadToVirtualThread) {
            Objects.requireNonNull(supplier, "supplier");
            try {
                return CompletableFuture.completedFuture(supplier.get());
            } catch (Exception failure) {
                CompletableFuture<T> failed = new CompletableFuture<>();
                failed.completeExceptionally(failure instanceof RuntimeException ? failure : new RuntimeException(failure));
                return failed;
            }
        }
    }

    private static final class ThreadLocalExecutionContextCarrier implements ExecutionContextCarrier {
        private static final ThreadLocal<java.util.Map<String, Object>> CONTEXT = ThreadLocal.withInitial(java.util.HashMap::new);
        private static final ThreadLocalExecutionContextCarrier INSTANCE = new ThreadLocalExecutionContextCarrier();

        static ThreadLocalExecutionContextCarrier instance() {
            return INSTANCE;
        }

        private ThreadLocalExecutionContextCarrier() {
        }

        @Override
        public <T> T get(String key, Class<T> type) {
            Object value = CONTEXT.get().get(key);
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
            if (value == null) {
                CONTEXT.get().remove(key);
            } else {
                CONTEXT.get().put(key, value);
            }
        }

        @Override
        public void clear(String key) {
            if (key == null) {
                return;
            }
            CONTEXT.get().remove(key);
        }

        @Override
        public void clear() {
            CONTEXT.remove();
        }
    }
}
